<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Exception;
use Psr\SimpleCache\CacheInterface;
use rabbit\App;
use rabbit\contract\InitInterface;
use rabbit\core\BaseObject;
use rabbit\helper\ArrayHelper;
use rabbit\helper\ExceptionHelper;
use rabbit\helper\VarDumper;
use rabbit\memory\atomic\AtomicLock;
use rabbit\memory\atomic\LockInterface;
use rabbit\redis\Redis;

/**
 * Interface AbstractPlugin
 * @package Rabbit\Data\Pipeline
 */
abstract class AbstractPlugin extends BaseObject implements InitInterface
{
    const LOG_SIMPLE = 0;
    const LOG_INFO = 1;
    /** @var string */
    public $taskName;
    /** @var string */
    public $task_id;
    /** @var string */
    public $key;
    /** @var array */
    protected $config = [];
    /** @var mixed */
    public $input;
    /** @var array */
    public $request = [];
    /** @var array */
    public $opt = [];
    /** @var array */
    protected $output = [];
    /** @var bool */
    protected $start = false;
    /** @var Redis */
    public $redis;
    /** @var int */
    protected $lockEx = 0;
    /** @var CacheInterface */
    protected $cache;
    /** @var string */
    const CACHE_KEY = 'cache';
    /** @var string */
    const LOCK_KEY = 'Plugin';
    /** @var string */
    protected $schedulerName = 'scheduler';
    /** @var int */
    protected $logInfo = self::LOG_SIMPLE;
    /** @var LockInterface */
    protected $atomicLock;
    /** @var callable */
    protected $errHandler;

    /**
     * AbstractPlugin constructor.
     * @param array $config
     * @throws Exception
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->redis = getDI('redis');
        $this->atomicLock = new AtomicLock();
    }

    public function init()
    {
        $this->cache = getDI(self::CACHE_KEY);
        $this->errHandler = ArrayHelper::getValue($this->config, 'errHandler');
    }

    /**
     * @return bool
     */
    public function getStart(): bool
    {
        return $this->start;
    }

    /**
     * @return int
     */
    public function getLock(string $key = null): bool
    {
        if ($key || $key = $this->task_id) {
            return (bool)$this->redis->set($key, true, ['nx', 'ex' => $this->lockEx]);
        }
        return true;
    }

    /**
     * @param \Closure $function
     * @param array $params
     * @throws Exception
     */
    public function redisLock(string $key, \Closure $function, array $params)
    {
        try {
            if ($this->redis->setnx($key, true)) {
                return call_user_func_array($function, $params);
            }
            return null;
        } catch (\Throwable $exception) {
            App::error(ExceptionHelper::dumpExceptionToString($exception));
        } finally {
            $this->redis->del($key);
        }
    }

    /**
     * @param string $key
     * @return mixed|null
     */
    public function getFromInput(string $key)
    {
        return ArrayHelper::getValue($this->input, $key);
    }

    /**
     * @param string $key
     * @return mixed|null
     */
    public function getFromOpt(string $key)
    {
        return ArrayHelper::getValue($this->opt, $key);
    }

    /**
     * @param string $lockKey
     * @return bool
     */
    public function deleteLock(string $key = null): int
    {
        return $this->redis->del($key ?? $this->task_id);
    }

    /**
     * @throws Exception
     */
    public function process(): void
    {
        try {
            $this->run();
        } catch (\Throwable $exception) {
            if (!is_array($this->errHandler)) {
                $this->errHandler = [$this->errHandler];
            }
            foreach ($this->errHandler as $handle) {
                if (is_callable($handle)) {
                    call_user_func($handle, $this, $exception);
                } elseif ($handle instanceof ErrorHandleInterface) {
                    $handle->handle($this, $exception);
                } else {
                    App::error(ExceptionHelper::dumpExceptionToString($exception));
                }
            }
            throw $exception;
        }
    }

    abstract public function run();

    /**
     * @param $data
     * @throws Exception
     */
    public function output(&$data, int $workerId = null): void
    {
        foreach ($this->output as $output => $transfer) {
            if (is_bool($transfer)) {
                if ($transfer === false) {
                    $transfer = null;
                } else {
                    $transfer = -1;
                }
            }
            if (empty($data)) {
                App::warning("Road from $this->key to $output with empty data", 'Data');
            } elseif ($this->logInfo === self::LOG_SIMPLE) {
                App::info("Road from $this->key to $output", 'Data');
            } else {
                App::info("Road from $this->key to $output with data " . VarDumper::getDumper()->dumpAsString($data), 'Data');
            }
            getDI($this->schedulerName)->send($this->taskName, $output, $this->task_id, $data, $workerId ?? $transfer, $this->opt, $this->request);
        }
    }
}
