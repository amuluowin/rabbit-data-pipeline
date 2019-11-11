<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Exception;
use Psr\SimpleCache\CacheInterface;
use rabbit\App;
use rabbit\contract\InitInterface;
use rabbit\core\BaseObject;
use rabbit\core\Context;
use rabbit\helper\ArrayHelper;
use rabbit\helper\VarDumper;
use rabbit\redis\Redis;

/**
 * Class AbstractSingletonPlugin
 * @package Rabbit\Data\Pipeline
 */
abstract class AbstractSingletonPlugin extends BaseObject implements InitInterface
{
    const LOG_SIMPLE = 0;
    const LOG_INFO = 1;
    /** @var string */
    public $taskName;
    /** @var string */
    public $key;
    /** @var array */
    protected $config = [];
    /** @var array */
    protected $output = [];
    /** @var bool */
    protected $start = false;
    /** @var string */
    protected $logKey = 'Plugin';
    /** @var Redis */
    public $redis;
    /** @var int */
    protected $lockEx = 0;
    /** @var CacheInterface */
    protected $cache;
    /** @var string */
    const CACHE_KEY = 'cache';
    /** @var string */
    protected $scheduleName = 'singletonscheduler';
    /** @var int */
    protected $logInfo = LOG_SIMPLE;

    /**
     * AbstractPlugin constructor.
     * @param array $config
     * @throws Exception
     */
    public function __construct(array $config)
    {
        $this->config = $config;
        $this->redis = getDI('redis');
    }

    public function init()
    {
        $this->cache = getDI(self::CACHE_KEY);
    }

    /**
     * @return bool
     */
    public function getStart(): bool
    {
        return $this->start;
    }

    /**
     * @return string
     */
    public function getTask_id(): string
    {
        return (string)Context::get($this->taskName . $this->key . 'task_id');
    }

    /**
     * @param string $task_id
     */
    public function setTask_id(string $task_id): void
    {
        Context::set($this->taskName . $this->key . 'task_id', $task_id);
    }

    /**
     * @return int
     */
    public function getLock(string $key = null): bool
    {
        if ($key || $key = $this->getTask_id()) {
            return (bool)$this->redis->set($key, true, ['nx', 'ex' => $this->lockEx]);
        }
        return true;
    }

    /**
     * @param array $input
     * @param string $key
     * @return mixed|null
     */
    public function getFromInput(array &$input, string $key)
    {
        return ArrayHelper::getValue($input, $key);
    }

    /**
     * @param string $key
     * @return mixed|null
     */
    public function getFromOpt(string $key)
    {
        return ArrayHelper::getValue($this->getOpt(), $key);
    }

    /**
     * @param string $lockKey
     * @return bool
     */
    public function deleteLock(string $key = null): int
    {
        return $this->redis->del($key ?? $this->getTask_id());
    }

    /**
     * @param array $opt
     */
    public function setOpt(array $opt): void
    {
        Context::set($this->getTask_id() . 'opt', $opt);
    }

    /**
     * @return array
     */
    public function getOpt(): array
    {
        return (array)Context::get($this->getTask_id() . 'opt');
    }

    /**
     * @param $input
     * @param array $opt
     */
    public function process(&$input, array &$opt)
    {
        $this->setOpt($opt);
        $this->run($input);
    }

    /**
     * @param $input
     * @return mixed
     */
    abstract public function run(&$input);

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
            if ($this->logInfo === self::LOG_SIMPLE) {
                App::info("Road from $this->key to $output", 'Data');
            } else {
                App::info("Road from $this->key to $output with data " . VarDumper::getDumper()->dumpAsString($data), 'Data');
            }
            getDI($this->scheduleName)->send($this->taskName, $output, $this->getTask_id(), $data, $workerId ?? $transfer, $this->getOpt());
        }
    }
}
