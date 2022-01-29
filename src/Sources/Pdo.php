<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\DBHelper;
use Rabbit\DB\MakePdoConnection;
use Rabbit\DB\Query;
use ReflectionException;
use Throwable;

/**
 * Class Pdo
 * @package Rabbit\Data\Pipeline\Sources
 */
class Pdo extends AbstractPlugin
{
    protected $sql;
    protected string $dbName;
    protected int $duration;
    protected string $query;
    protected ?int $each = null;
    protected array $params = [];
    protected string $cacheDriver = 'memory';

    /**
     * @param string $class
     * @param string $dsn
     * @param array $pool
     * @param array $retryHandler
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Throwable
     */
    protected function createConnection(string $class, string $dsn, array $pool, array $retryHandler): void
    {
        [
            $poolConfig['min'],
            $poolConfig['max'],
            $poolConfig['wait'],
            $poolConfig['retry']
        ] = ArrayHelper::getValueByArray(
            $pool,
            ['min', 'max', 'wait', 'retry'],
            [10, 12, 0, 3]
        );
        MakePdoConnection::addConnection($class, $this->dbName, $dsn, $poolConfig, $retryHandler);
    }

    /**
     * @return mixed|void
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $this->dbName,
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
            ['dbName', 'class', 'dsn', 'pool', 'retryHandler', self::CACHE_KEY, 'sql', 'duration', 'query', 'each', 'params'],
            [
                'dbName' => null,
                self::CACHE_KEY => 'memory',
                'query' => 'queryAll',
                'duration' => -1,
                'pool' => [],
                'retryHandler' => [],
                'each' => false,
                'params' => []
            ]
        );
        if ($this->sql === null) {
            throw new InvalidConfigException("sql must be set in $this->key");
        }
        if (!$this->dbName) {
            if ($dsn === null || $class === null) {
                throw new InvalidConfigException("when not set dbName then dsn & class must be set in $this->key");
            }
            $this->dbName = md5($dsn);
            $this->createConnection($class, $dsn, $pool, $retryHandler);
        }
    }

    /**
     * @param Message $msg
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Throwable
     * @throws ReflectionException
     */
    public function run(Message $msg): void
    {
        if (is_array($this->sql)) {
            $batchNum = ArrayHelper::remove($this->sql, 'batch');
            $this->sql = ArrayHelper::merge([self::CACHE_KEY => $this->duration], $this->sql);
            if ($batchNum) {
                foreach (DBHelper::Search(new Query(service('db')->get($this->dbName)), $this->sql)->batch($batchNum) as $list) {
                    wgeach($list, function (int $key, array $datum) use ($msg): void {
                        $tmp = clone $msg;
                        $tmp->data = $datum;
                        $this->send($tmp);
                    });
                }
            } else {
                $msg->data = DBHelper::PubSearch(new Query(service('db')->get($this->dbName)), $this->sql, $this->query);
                $this->send($msg);
            }
        } else {
            $params = $this->makeParams($msg);
            $msg->data = service('db')->get($this->dbName)->createCommand($this->sql, $params)->cache($this->duration, $this->cache->getDriver($this->cacheDriver))->{$this->query}();
            $this->send($msg);
        }
    }

    /**
     * @param Message $msg
     * @return array
     */
    protected function makeParams(Message $msg): array
    {
        $params = [];
        foreach ($this->params as $key => $value) {
            switch ($value) {
                case 'getFromInput':
                    $params[] = ArrayHelper::getValue($msg->data, $key);
                    break;
                case 'input':
                    $params[] = json_encode($msg->data, JSON_UNESCAPED_UNICODE);
                    break;
                default:
                    if (method_exists($this, $value)) {
                        $params[] = $this->$value();
                    } else {
                        $params[] = $value;
                    }
            }
        }
        return $params;
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    protected function send(Message $msg): void
    {
        if (ArrayHelper::isIndexed($msg->data) && $this->each) {
            foreach ($msg->data as $item) {
                $tmp = clone $msg;
                $tmp->data = $item;
                $this->sink($tmp);
            }
        } else {
            $this->sink($msg);
        }
    }
}
