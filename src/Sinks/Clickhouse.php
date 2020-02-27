<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Co\System;
use DI\DependencyException;
use DI\NotFoundException;
use rabbit\App;
use rabbit\core\Context;
use rabbit\core\Exception;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\db\clickhouse\BatchInsert;
use rabbit\db\clickhouse\BatchInsertCsv;
use rabbit\db\clickhouse\BatchInsertJsonRows;
use rabbit\db\clickhouse\Connection;
use rabbit\db\clickhouse\MakeCKConnection;
use rabbit\db\ConnectionInterface;
use rabbit\db\Expression;
use rabbit\exception\InvalidArgumentException;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;

/**
 * Class Clickhouse
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Clickhouse extends AbstractPlugin
{
    /** @var Connection */
    protected $db;
    /** @var string */
    protected $tableName;
    /** @var array */
    protected $primaryKey;
    /** @var string */
    protected $flagField;
    /** @var int */
    protected $maxCount = 10000;

    /**
     * @return mixed|void
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     */
    public function init()
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
            null,
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
        $driver = MakeCKConnection::addConnection($class, $dbName, $dsn, $config);
        $this->db = getDI($driver)->getConnection($dbName);
    }

    /**
     * @throws \Exception
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
        } catch (\Throwable $e) {
            App::error($e);
            throw $e;
        } finally {
            $this->deleteLock($lock);
        }

        $this->output($rows);
    }

    /**
     * @return int
     * @throws DependencyException
     * @throws NotFoundException
     * @throws \rabbit\db\Exception
     */
    protected function saveWithLine(): int
    {
        if (!ArrayHelper::isIndexed($this->input['data'])) {
            $this->input['data'] = [$this->input['data']];
        }
        if ($this->db instanceof Connection) {
//            $batch = count($this->input['data']) > $this->maxCount ? new BatchInsertCsv($this->tableName, uniqid(), $this->db) : new BatchInsert($this->tableName, $this->db);
            $batch =  new BatchInsert($this->tableName, $this->db);
            $batch->addColumns($this->input['columns']);
            foreach ($this->input['data'] as $item) {
                $batch->addRow($item);
            }
            $rows = $batch->execute();
        } else {
            $rows = $this->db->insert($this->tableName, $this->input['columns'], $this->input['data']);
        }
        App::warning("$this->tableName succ: $rows");
        return $rows;
    }

    protected function getUpdateFlagCondition()
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
     * @param array $ids
     * @throws \rabbit\db\Exception
     */
    protected function updateFlag(array $updateFlagCondition): void
    {
        if ($this->db instanceof Connection) {
            $model = new class($this->tableName, $this->db) extends \rabbit\db\clickhouse\ActiveRecord {
                /**
                 *  constructor.
                 * @param string $tableName
                 * @param string $dbName
                 */
                public function __construct(string $tableName, ConnectionInterface $db)
                {
                    Context::set(md5(get_called_class() . 'tableName'), $tableName);
                    Context::set(md5(get_called_class() . 'db'), $db);
                }

                /**
                 * @return mixed|string
                 */
                public static function tableName()
                {
                    return Context::get(md5(get_called_class() . 'tableName'));
                }

                /**
                 * @return ConnectionInterface
                 */
                public static function getDb(): ConnectionInterface
                {
                    return Context::get(md5(get_called_class() . 'db'));
                }
            };
        } else {
            $model = new class($this->tableName, $this->db) extends \rabbit\db\click\ActiveRecord {
                /**
                 *  constructor.
                 * @param string $tableName
                 * @param string $dbName
                 */
                public function __construct(string $tableName, ConnectionInterface $db)
                {
                    Context::set(md5(get_called_class() . 'tableName'), $tableName);
                    Context::set(md5(get_called_class() . 'db'), $db);
                }

                /**
                 * @return mixed|string
                 */
                public static function tableName()
                {
                    return Context::get(md5(get_called_class() . 'tableName'));
                }

                /**
                 * @return ConnectionInterface
                 */
                public static function getDb(): ConnectionInterface
                {
                    return Context::get(md5(get_called_class() . 'db'));
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
