<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use Psr\SimpleCache\CacheInterface;
use rabbit\db\Connection;
use rabbit\db\DBHelper;
use rabbit\db\MakePdoConnection;
use rabbit\db\Query;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;

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
    /** @var CacheInterface */
    protected $cache;
    /** @var int */
    protected $duration;
    /** @var string */
    protected $query;
    /** @var Connection */
    protected $db;
    /** @var int */
    protected $each = false;

    const CACHE_KEY = 'cache';

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
            [10, 13, 0, 3]
        );
        MakePdoConnection::addConnection($class, $this->dbName, $dsn, $poolConfig);
    }

    /**
     * @return mixed|void
     * @throws Exception
     */
    public function init()
    {
        [
            $class,
            $dsn,
            $pool,
            $this->sql,
            $cache,
            $this->duration,
            $this->query,
            $this->each
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'pool', 'sql', self::CACHE_KEY, 'duration', 'query', 'each'],
            null,
            [
                self::CACHE_KEY => 'memory',
                'query' => 'queryAll',
                'duration' => null,
                'pool' => [],
                'each' => false
            ]
        );
        if ($dsn === null || $class === null || $this->sql === null) {
            throw new InvalidConfigException("class and dsn must be set in $this->key");
        }
        $this->dbName = md5($dsn);
        $this->cache = getDI(self::CACHE_KEY)->getDriver($cache);
        $this->createConnection($class, $dsn, $pool);
        $this->db = getDI('db')->getConnection($this->dbName);
    }

    /**
     * @param $input
     * @throws Exception
     */
    public function input(&$input = null): void
    {
        if (is_array($this->sql)) {
            $batch = ArrayHelper::remove($this->sql, 'batch');
            if ($batch) {
                foreach (DBHelper::Search(new Query(), $this->sql)->batch($batch) as $batchList) {
                    $this->send($batchList);
                }
            } else {
                $data = DBHelper::PubSearch(new Query(), $this->sql, $this->query);
                $this->send($data);
            }
        } else {
            $data = $this->db->createCommand($this->sql)->cache($this->duration, $this->cache)->{$this->query}();
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
                $this->output($item);
            }
        } else {
            $this->output($data);
        }
    }
}
