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
    /** @var array */
    private $ids = [];
    /** @var bool */
    private $isLine = false;
    /** @var BatchInsert */
    private $batch;

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
        MakeCKConnection::addConnection($class, $dbName, $dsn, $config);
        $this->db = getDI('clickhouse')->getConnection($dbName);
    }

    /**
     * @throws \Exception
     */
    public function run()
    {
        if (isset($this->input['columns'])) {
            $this->isLine = true;
        }
        if ($this->isLine) {
            $this->saveWithLine();
        } else {
            $this->saveWithRows();
        }
        if ($this->primaryKey && $this->ids) {
            $sql = <<<'SQL'
ALTER TABLE {$this->tableName} UPDATE {$this->flagField}={$this->flagField}+1 
WHERE ({$this->flagField}=0 or {$this->flagField}=1) AND {$this->primaryKey} in ({$this->ids})
SQL;
            $this->db->createCommand($sql)->execute();
        }
    }

    /**
     * @throws \Exception
     */
    protected function saveWithLine(): void
    {
        $this->batch === null && ($this->batch = new BatchInsert($this->tableName, getDI('db')->getConnection($this->dbName)));
        if (isset($this->input['columns'])) {
            $this->batch->addColumns($this->input['columns']);
        } elseif ($this->input) {
            if ($this->primaryKey) {
                $this->ids[] = $this->input[array_search($this->batch->getColumns()[$this->primaryKey])];
            }
            $this->batch->addRow($this->input);
        } else {
            $this->batch->execute();
            $this->batch->clearData();
        }
    }

    protected function saveWithRows(): void
    {
        $this->batch === null && ($this->batch = new BatchInsertJsonRows($this->tableName, $this->db));
        if ($this->input) {
            if ($this->primaryKey) {
                $this->ids[] = $this->input[$this->primaryKey];
            }
            $this->batch->addRow($this->input);
        } else {
            $this->batch->execute();
            $this->batch->clearData();
        }
    }
}