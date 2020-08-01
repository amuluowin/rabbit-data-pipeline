<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Co\System;
use Rabbit\Base\App;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\ClickHouse\ActiveRecord;
use Rabbit\DB\ClickHouse\BatchInsertCsv;
use Rabbit\DB\ClickHouse\MakeCKConnection;
use Rabbit\DB\Expression;
use Rabbit\Pool\ConnectionInterface;
use Throwable;

/**
 * Class Clickhouse
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Clickhouse extends AbstractPlugin
{
    protected string $db;
    protected ?string $tableName = null;
    protected ?array $primaryKey;
    protected string $flagField;
    protected int $maxCount;
    protected string $driver;

    /**
     * @return mixed|void
     * @throws InvalidConfigException
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $class,
            $dsn,
            $config,
            $this->tableName,
            $this->flagField,
            $this->primaryKey,
            $this->maxCount
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'config', 'tableName', 'flagField', 'primaryKey', 'maxCount'],
            [
                'config' => [],
                'flagField' => 'flag',
                'maxCount' => 10000,
            ]
        );
        if ($dsn === null || $class === null || $this->primaryKey === null) {
            throw new InvalidConfigException("class, dsn, primaryKey must be set in $this->key");
        }
        $dbName = md5($dsn);
        $this->driver = MakeCKConnection::addConnection($class, $dbName, $dsn, $config);
        $this->db = $dbName;
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        if (empty($this->tableName) && isset($msg->data['tableName'])) {
            $this->tableName = $msg->data['tableName'];
        }
        if (empty($this->tableName) && isset($msg->opt['tableName'])) {
            $this->tableName = $msg->opt['tableName'];
        }
        if (isset($msg->data['flagField'])) {
            $this->flagField = $msg->data['flagField'];
        }
        if (isset($msg->data['primaryKey'])) {
            $this->primaryKey = $msg->data['primaryKey'];
        }
        if (!isset($msg->data['columns']) || !isset($msg->data['data']) || empty($this->tableName)) {
            throw new InvalidArgumentException("Get Args Failed: 「columus」「data」「tableName」");
        }

        // 获取update flag的条件
        [$updateFlagCondition, $lock] = $this->getUpdateFlagCondition($msg);
        try {
            // 设置redis锁， 防止同时插入更新
            if ($this->primaryKey && !empty($updateFlagCondition)) {
                while (!$msg->getLock($lock)) {
                    App::warning("wait update $this->flagField lock: $lock");
                    System::sleep(1);
                }
            }
            // 存储数据
            $msg->data = $this->saveWithLine($msg);
            App::warning("$this->tableName insert succ: $rows");

            // 更新flag 删除锁
            if ($this->primaryKey && !empty($updateFlagCondition) && $rows > 0 && isset($lock)) {
                $this->updateFlag($updateFlagCondition);
                App::warning("update $this->flagField succ:  $lock");
            }
        } catch (Throwable $e) {
            App::error($e);
            throw $e;
        } finally {
            $msg->deleteLock($lock);
        }
        $this->sink($msg);
    }

    /**
     * @param Message $msg
     * @return int
     * @throws Exception
     * @throws Throwable
     */
    protected function saveWithLine(Message $msg): int
    {
        if (!ArrayHelper::isIndexed($msg->data['data'])) {
            $msg->data['data'] = [$msg->data['data']];
        }
        if ($this->driver === 'clickhouse') {
            $batch = new BatchInsertCsv(
                $this->tableName,
                strval(getDI('idGen')->create()),
                getDI($this->driver)->get($this->db)
            );
            $batch->addColumns($msg->data['columns']);
            foreach ($msg->data['data'] as $item) {
                $batch->addRow($item);
            }
            $rows = $batch->execute();
        } else {
            $rows = getDI($this->driver)->get($this->db)->insert($this->tableName, $msg->data['columns'], $msg->data['data']);
        }
        App::warning("$this->tableName success: $rows");
        return $rows;
    }

    /**
     * @param Message $msg
     * @return array
     */
    protected function getUpdateFlagCondition(Message $msg): array
    {
        $result = [];
        $lock = '';
        if (!is_array($this->primaryKey)) {
            $this->primaryKey = [$this->primaryKey];
        }
        foreach ($this->primaryKey as $field) {
            $fieldValue = array_unique(ArrayHelper::getColumn($msg->data['data'], array_search($field, $msg->data['columns']), []));
            $result[$field] = $fieldValue;
            $lock .= "{$field}" . implode('', $fieldValue);
        }
        $lock = $this->tableName . ':' . md5($lock);
        return [$result, $lock];
    }


    /**
     * @param array $updateFlagCondition
     * @throws Exception
     * @throws Throwable
     */
    protected function updateFlag(array $updateFlagCondition): void
    {
        $model = new class($this->driver, $this->tableName, $this->db) extends ActiveRecord {
            /**
             *  constructor.
             * @param string $driver
             * @param string $tableName
             * @param string $db
             */
            public function __construct(string $driver, string $tableName, string $db)
            {
                Context::set(md5(get_called_class() . 'driver'), $driver);
                Context::set(md5(get_called_class() . 'tableName'), $tableName);
                Context::set(md5(get_called_class() . 'db'), $db);
            }

            /**
             * @return mixed|string
             */
            public static function tableName(): string
            {
                return Context::get(md5(get_called_class() . 'tableName'));
            }

            /**
             * @return ConnectionInterface
             */
            public static function getDb(): ConnectionInterface
            {
                return getDI(Context::get(md5(get_called_class() . 'driver')))->get(Context::get(md5(get_called_class() . 'db')));
            }
        };

        $res = $model::updateAll([$this->flagField => new Expression("{$this->flagField}+1")], array_merge([
            $this->flagField => [0, 1]
        ], $updateFlagCondition));
        if (!empty($res) && $res !== true) {
            throw new Exception($res);
        }
    }
}
