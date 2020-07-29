<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\ConnectionInterface;
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
    /** @var string|array */
    protected $sql;
    /** @var string */
    protected string $dbName;
    /** @var int */
    protected int $duration;
    /** @var string */
    protected string $query;
    /** @var ConnectionInterface */
    protected ConnectionInterface $db;
    /** @var int */
    protected ?int $each = null;
    /** @var array */
    protected array $params = [];
    /** @var string */
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
     * @param Message $msg
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Throwable
     * @throws ReflectionException
     */
    public function run(Message $msg): void
    {
        if (is_array($this->sql)) {
            $batch = ArrayHelper::remove($this->sql, 'batch');
            $this->sql = ArrayHelper::merge([self::CACHE_KEY => $this->duration], $this->sql);
            if ($batch) {
                foreach (DBHelper::Search(new Query(getDI('db')->get($this->dbName)), $this->sql)->batch($batch) as $msg->data) {
                    $this->send($msg);
                }
            } else {
                $msg->data = DBHelper::PubSearch(new Query(getDI('db')->get($this->dbName)), $this->sql, $this->query);
                $this->send($msg);
            }
        } else {
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
            $msg->data = getDI('db')->get($this->dbName)->createCommand($this->sql, $params)->cache($this->duration, $this->cache->getDriver($this->cacheDriver))->{$this->query}();
            $this->send($msg);
        }
    }

    /**
     * @param Message $msg
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws ReflectionException
     * @throws Throwable
     */
    protected function send(Message $msg): void
    {
        if (ArrayHelper::isIndexed($msg->data) && $this->each) {
            foreach ($msg->data as $item) {
                rgo(function () use (&$item, $msg) {
                    $tmp = clone $msg;
                    $tmp->data = $item;
                    $this->sink($tmp);
                });
            }
        } else {
            $this->sink($msg);
        }
    }
}
