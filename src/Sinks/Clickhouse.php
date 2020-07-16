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
    /** @var string */
    protected string $db;
    /** @var string */
    protected ?string $tableName = null;
    /** @var array */
    protected ?array $primaryKey;
    /** @var string */
    protected string $flagField;
    /** @var int */
    protected int $maxCount;
    /** @var string */
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
     * @throws Throwable
     */
    public function run()
    {
        if (empty($this->tableName) && isset($this->input['tableName'])) {
            $this->tableName = $this->input['tableName'];
        }
        if (empty($this->tableName) && isset($this->opt['tableName'])) {
            $this->tableName = $this->opt['tableName'];
        }
        if (isset($this->input['flagField'])) {
            $this->flagField = $this->input['flagField'];
        }
        if (isset($this->input['primaryKey'])) {
            $this->primaryKey = $this->input['primaryKey'];
        }
        if (!isset($this->input['columns']) || !isset($this->input['data']) || empty($this->tableName)) {
            throw new InvalidArgumentException("Get Args Failed: 「columus」「data」「tableName」");
        }

        try {
            // 获取update flag的条件
            [$updateFlagCondition, $lock] = $this->getUpdateFlagCondition();

            // 设置redis锁， 防止同时插入更新
            if ($this->primaryKey && !empty($updateFlagCondition)) {
                while (!$this->getLock($lock)) {
                    App::warning("wait update $this->flagField lock: $lock");
                    System::sleep(1);
                }
            }
            // 存储数据
            $rows = $this->saveWithLine();
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
            $this->deleteLock($lock);
        }

        $this->output($rows);
    }

    /**
     * @return int
     * @throws Exception
     * @throws Throwable
     */
    protected function saveWithLine(): int
    {
        if (!ArrayHelper::isIndexed($this->input['data'])) {
            $this->input['data'] = [$this->input['data']];
        }
        if ($this->driver === 'clickhouse') {
            $batch = new BatchInsertCsv(
                $this->tableName,
                strval(getDI('idGen')->create()),
                getDI($this->driver)->get($this->db)
            );
            $batch->addColumns($this->input['columns']);
            foreach ($this->input['data'] as $item) {
                $batch->addRow($item);
            }
            $rows = $batch->execute();
        } else {
            $rows = getDI($this->driver)->get($this->db)->insert($this->tableName, $this->input['columns'], $this->input['data']);
        }
        App::warning("$this->tableName success: $rows");
        return $rows;
    }

    /**
     * @return array
     */
    protected function getUpdateFlagCondition(): array
    {
        $result = [];
        $lock = '';
        if (!is_array($this->primaryKey)) {
            $this->primaryKey = [$this->primaryKey];
        }
        foreach ($this->primaryKey as $field) {
            $fieldValue = array_unique(ArrayHelper::getColumn($this->input['data'], array_search($field, $this->input['columns']), []));
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
        if ($this->driver === 'clickhouse') {
            $model = new class($this->tableName, $this->db) extends ActiveRecord {
                /**
                 *  constructor.
                 * @param string $tableName
                 * @param string $db
                 */
                public function __construct(string $tableName, string $db)
                {
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
                    return getDI('clickhouse')->get(Context::get(md5(get_called_class() . 'db')));
                }
            };
        } else {
            $model = new class($this->tableName, $this->db) extends \Rabbit\DB\Click\ActiveRecord {
                /**
                 *  constructor.
                 * @param string $tableName
                 * @param string $db
                 */
                public function __construct(string $tableName, string $db)
                {
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
                    return getDI('click')->get(Context::get(md5(get_called_class() . 'db')));
                }
            };
        }

        $res = $model::updateAll([$this->flagField => new Expression("{$this->flagField}+1")], array_merge([
            $this->flagField => [0, 1]
        ], $updateFlagCondition));
        if (!empty($res) && $res !== true) {
            ding()->at()->text("Update $this->flagField Failed; tableName: $this->tableName, condition: " . json_encode($updateFlagCondition, JSON_UNESCAPED_UNICODE));
            throw new Exception($res);
        }
    }
}
