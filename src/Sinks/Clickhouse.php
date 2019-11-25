<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;


use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\db\clickhouse\BatchInsert;
use rabbit\db\clickhouse\BatchInsertJsonRows;
use rabbit\db\clickhouse\Connection;
use rabbit\db\clickhouse\MakeCKConnection;
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
    /** @var string */
    protected $primaryKey;
    /** @var string */
    protected $flagField;

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
            $this->flagField
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'config', 'tableName', 'flagField'],
            null,
            [
                'config' => [],
                'flagField' => 'flag'
            ]
        );
        if ($dsn === null || $class === null || $this->tableName === null) {
            throw new InvalidConfigException("class, dsn must be set in $this->key");
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
        if (isset($this->input['columns'])) {
            $ids = $this->saveWithLine();
        } else {
            $ids = $this->saveWithRows();
        }
        if ($this->primaryKey && $this->ids) {
            $sql = <<<'SQL'
ALTER TABLE {$this->tableName} UPDATE {$this->flagField}={$this->flagField}+1 
WHERE ({$this->flagField}=0 or {$this->flagField}=1) AND {$this->primaryKey} in ({$ids})
SQL;
            $this->db->createCommand($sql)->execute();
        }
    }

    /**
     * @return array
     * @throws DependencyException
     * @throws NotFoundException
     * @throws \rabbit\db\Exception
     */
    protected function saveWithLine(): array
    {
        if ($this->db->createCommand()->batchInsert($this->tableName, $this->input['columns'], $this->input['data'])->execute()) {
            return ArrayHelper::getColumn($this->input['data'], $this->primaryKey, []);
        }
        return [];
    }

    protected function saveWithRows(): array
    {
        $batch = new BatchInsertJsonRows($this->tableName, $this->db);
        $batch->addColumns($this->input['columns']);
        $batch->addRow($this->input['data']);
        if ($batch->execute()) {
            return ArrayHelper::getColumn($this->input['data'], $this->primaryKey, []);
        }
        return [];
    }
}