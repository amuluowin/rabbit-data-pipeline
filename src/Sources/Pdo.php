<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\db\Connection;
use rabbit\db\DBHelper;
use rabbit\db\MakePdoConnection;
use rabbit\db\Query;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;

/**
 * Class Pdo
 * @package Rabbit\Data\Pipeline\Sources
 */
class Pdo extends AbstractPlugin
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
            [10, 13, 0, 3]
        );
        MakePdoConnection::addConnection($class, $this->dbName, $dsn, $retryHandler, $poolConfig);
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
            $retryHandler,
            $this->cacheDriver,
            $this->sql,
            $this->duration,
            $this->query,
            $this->each,
            $this->params
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'pool', 'retryHandler', self::CACHE_KEY, 'sql', 'duration', 'query', 'each', 'params'],
            null,
            [
                self::CACHE_KEY => 'memory',
                'query' => 'queryAll',
                'duration' => -1,
                'pool' => [],
                'retryHandler' => [],
                'each' => false,
                'params' => []
            ]
        );
        if ($dsn === null || $class === null || $this->sql === null) {
            throw new InvalidConfigException("class, dsn and sql must be set in $this->key");
        }
        $this->dbName = md5($dsn);
        $this->createConnection($class, $dsn, $pool, $retryHandler);
    }

    /**
     * @throws Exception
     */
    public function run(): void
    {
        if (is_array($this->sql)) {
            $batch = ArrayHelper::remove($this->sql, 'batch');
            $this->sql = ArrayHelper::merge([self::CACHE_KEY => $this->duration], $this->sql);
            if ($batch) {
                foreach (DBHelper::Search(new Query(), $this->sql)->batch($batch) as $batchList) {
                    $this->send($batchList);
                }
            } else {
                $data = DBHelper::PubSearch(new Query(), $this->sql, $this->query, getDI('db')->getConnection($this->dbName));
                $this->send($data);
            }
        } else {
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
            $data = getDI('db')->getConnection($this->dbName)->createCommand($this->sql, $params)->cache($this->duration, $this->cache->getDriver($this->cacheDriver))->{$this->query}();
            $this->send($data);
        }
    }

    /**
     * @param $data
     * @throws Exception
     */
    protected function send(&$data): void
    {
        if (ArrayHelper::isIndexed($data) && $this->each) {
            foreach ($data as $item) {
                rgo(function () use (&$item) {
                    $this->output($item);
                });
            }
        } else {
            $this->output($data);
        }
    }
}
