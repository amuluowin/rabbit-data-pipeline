<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use common\Exception\IgnoreException;
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
    public $locks = [];
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
    /** @var bool */
    protected $wait = false;
    /** @var string */
    protected $pluginName;

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
            if ((bool)$this->redis->set($key, true, ['nx', 'ex' => $this->lockEx])) {
                $this->opt['Locks'][] = $key;
                return true;
            }
        }
        return false;
    }

    /**
     * @param $key
     * @return string
     */
    public function makeLockKey($key): string
    {
        is_array($key) && $key = implode('_', $key);
        if (!is_string($key)) {
            throw new Exception("lockKey Must be string or array");
        }
        return 'Locks:' . $key;
    }

    public function deleteAllLock()
    {
        $locks = isset($this->opt['Locks']) ? $this->opt['Locks'] : [];
        foreach ($locks as $lock) {
            $this->deleteLock($lock);
        }
    }

    /**
     * @param string $lockKey
     * @return bool
     */
    public function deleteLock(string $key = null): int
    {
        ($key === null) && $key = $this->task_id;
        if ($flag = $this->redis->del($key)) {
            App::warning("「{$this->taskName}」 Delete Lock: " . $key);
        }
        return $flag;

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
     * @throws Exception
     */
    public function process(): void
    {
        try {
            $this->run();
        } catch (\Throwable $exception) {
            if (empty($this->errHandler)) {
                throw $exception;
            }
            if (!is_array($this->errHandler)) {
                $this->errHandler = [$this->errHandler];
            }
            //删除锁
            $this->deleteAllLock();

            $errerrHandler = $this->errHandler;
            self::dealException($errerrHandler, $exception);
        }
    }

    public function dealException(&$errerrHandler, $exception)
    {
        while (!empty($errerrHandler)) {
            try {
                $handle = array_shift($errerrHandler);
                if (is_callable($handle)) {
                    call_user_func($handle, $this, $exception);
                } else {
                    throw $exception;
                }
            } catch (\Throwable $exception) {
                throw $exception;
            }
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
                App::warning("「{$this->taskName}」 $this->key -> $output; data is empty", 'Data');
            } elseif ($this->logInfo === self::LOG_SIMPLE) {
                App::info("「{$this->taskName}」 $this->key -> $output;", 'Data');
            } else {
                App::info("「{$this->taskName}」 $this->key -> $output; data: " . VarDumper::getDumper()->dumpAsString($data), 'Data');
            }
            getDI($this->schedulerName)->send($this->taskName, $output, $this->task_id, $data, $workerId ?? $transfer, $this->opt, $this->request, $this->wait);
        }
    }
}
