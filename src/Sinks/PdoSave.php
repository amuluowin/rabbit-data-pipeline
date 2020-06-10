<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\db\Connection;
use rabbit\db\Exception;
use rabbit\db\MakePdoConnection;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;

/**
 * Class Pdo
 * @package Rabbit\Data\Pipeline\Sinks
 */
class PdoSave extends AbstractPlugin
{
    /** @var string */
    protected $tableName;
    /** @var string */
    protected $dbName;

    /**
     * @param string $class
     * @param string $dsn
     * @param array $pool
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Exception
     */
    private function createConnection(string $class, string $dsn, array $pool): void
    {
        [
            $poolConfig['min'],
            $poolConfig['max'],
            $poolConfig['wait'],
            $poolConfig['retry']
        ] = ArrayHelper::getValueByArray(
            $pool,
            ['min', 'max', 'wait', 'retry'],
            null,
            [10, 12, 0, 3]
        );
        MakePdoConnection::addConnection($class, $this->dbName, $dsn, $poolConfig);
    }

    /**
     * @return mixed|void
     * @throws Exception
     */
    public function init()
    {
        parent::init();
        [
            $class,
            $dsn,
            $pool,
            $this->tableName
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'pool', 'tableName'],
            null,
            [
                'pool' => [],
            ]
        );
        if ($dsn === null || $class === null || $this->taskName === null) {
            throw new InvalidConfigException("class, dsn must be set in $this->key");
        }
        $this->dbName = md5($dsn);
        $this->createConnection($class, $dsn, $pool);
    }

    /**
     * @throws Exception
     */
    public function run(): void
    {
        if (empty($this->tableName) && isset($this->opt['tableName'])) {
            $this->tableName = $this->opt['tableName'];
        }
        if (isset($this->input['columns'])) {
            $this->saveWithLine();
        } else {
            $this->saveWithOne();
        }
    }

    /**
     * @throws Exception
     */
    protected function saveWithLine(): void
    {
        /** @var Connection $db */
        $db = getDI('db')->getConnection($this->dbName);
        $res = $db->createCommand()->batchInsert($this->tableName, $this->input['columns'], $this->input['data'])->execute();
        $this->output($res);
    }

    /**
     * @throws Exception
     */
    protected function saveWithOne(): void
    {
        $data = $this->input;
        if (!$res = getDI('db')->getConnection($this->dbName)
            ->createCommand()
            ->batchInsert($this->tableName, array_keys($data), [array_values($data)])
            ->execute()) {
            throw new Exception("save to $this->tableName failed");
        }
        $this->output($res);
    }
}
