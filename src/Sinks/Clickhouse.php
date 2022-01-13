<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Throwable;
use Rabbit\Base\App;
use Rabbit\DB\Expression;
use Rabbit\Base\Core\Exception;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\DB\ClickHouse\ActiveRecord;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\DB\ClickHouse\BatchInsertCsv;
use Rabbit\DB\ClickHouse\MakeCKConnection;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\InvalidArgumentException;

/**
 * Class Clickhouse
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Clickhouse extends AbstractPlugin
{
    protected string $dbName;
    protected ?string $tableName = null;
    protected ?array $primaryKey;
    protected string $flagField;
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
            $pool,
            $this->dbName,
            $this->driver,
            $this->tableName,
            $this->flagField,
            $this->primaryKey
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'pool', 'dbName', 'driver', 'tableName', 'flagField', 'primaryKey'],
            [
                'pool' => [],
                'flagField' => 'flag'
            ]
        );
        if ($this->primaryKey === null) {
            throw new InvalidConfigException("primaryKey must be set in $this->key");
        }
        if (!$this->dbName && !$this->driver) {
            if ($dsn === null || $class === null) {
                throw new InvalidConfigException("when not set dbName then dsn & class must be set in $this->key");
            }
            $this->dbName = md5($dsn);
            $this->driver = MakeCKConnection::addConnection($class, $this->dbName, $dsn, $pool);
        } elseif (($this->dbName && !$this->driver) || (!$this->dbName && $this->driver)) {
            throw new InvalidConfigException("dbName & driver must both has value or has no value");
        }
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
                    sleep(1);
                }
            }
            // 存储数据
            $msg->data = $this->saveWithLine($msg);
            App::warning("$this->tableName insert succ: $msg->data");

            // 更新flag 删除锁
            if ($this->primaryKey && !empty($updateFlagCondition) && $msg->data > 0 && isset($lock)) {
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
                strval(getDI('idGen')->nextId()),
                getDI('db')->get($this->dbName)
            );
            $batch->addColumns($msg->data['columns']);
            foreach ($msg->data['data'] as $item) {
                $batch->addRow($item);
            }
            $rows = $batch->execute();
        } else {
            $rows = getDI('db')->get($this->dbName)->insert($this->tableName, $msg->data['columns'], $msg->data['data']);
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
        $model = new class($this->tableName, $this->dbName) extends ActiveRecord
        {
            /**
             *  constructor.
             * @param string $tableName
             * @param string $db
             */
            public function __construct(string $tableName, string $db)
            {
                $this->tableName = $tableName;
                $this->db = getDI('db')->get($db);
            }
        };

        $res = $model->updateAll([$this->flagField => new Expression("{$this->flagField}+1")], [
            $this->flagField => [0, 1], ...$updateFlagCondition
        ]);
        if (!empty($res) && $res !== true) {
            throw new Exception((string)$res);
        }
    }
}
