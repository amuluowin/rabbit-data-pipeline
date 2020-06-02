<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;


use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Data\Pipeline\Sources\Pdo;
use rabbit\db\Connection;
use rabbit\db\DBHelper;
use rabbit\db\MakePdoConnection;
use rabbit\db\Query;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;

/**
 * Class OrmDB
 * @package Rabbit\Data\Pipeline\Common
 */
class OrmDB extends Pdo
{
    /** @var string */
    protected $sql;
    /** @var string */
    protected $dbName;
    /** @var int */
    protected $duration;
    /** @var string */
    protected $query;
    /** @var Connection */
    protected $db;
    /** @var int */
    protected $each = false;
    /** @var array */
    protected $params = [];
    /** @var string */
    protected $cacheDriver = 'memory';

    /**
     * @param string $class
     * @param string $dsn
     * @param array $pool
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Exception
     */
    private function createConnection(string $class, string $dsn, array $pool, array $retryHandler): void
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
        MakePdoConnection::addConnection($class, $this->dbName, $dsn, $poolConfig, $retryHandler);
    }

    /**
     * @return mixed|void
     * @throws Exception
     */
    public function init()
    {
        parent::init();
        if (!is_array($this->sql)) {
            throw new InvalidConfigException("sql only support array");
        }
    }

    /**
     * @throws Exception
     */
    public function run(): void
    {
        $params = [];
        foreach ($this->params as $key => $value) {
            switch ($value) {
                case 'getFromInput':
                    $params[] = ArrayHelper::getValue($this->input, $key);
                    break;
                case 'input':
                    $params[] = json_encode($this->input, JSON_UNESCAPED_UNICODE);
                    break;
                default:
                    if (method_exists($this, $value)) {
                        $params[] = $this->$value();
                    } else {
                        $params[] = $value;
                    }
            }
        }
        $data = getDI('db')->getConnection($this->dbName)->createCommandExt($this->sql, $params)->cache($this->duration, $this->cache->getDriver($this->cacheDriver))->{$this->query}();
        $this->send($data);
    }
}