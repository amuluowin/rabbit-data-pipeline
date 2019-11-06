<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Exception;
use Psr\SimpleCache\CacheInterface;
use rabbit\App;
use rabbit\contract\InitInterface;
use rabbit\core\BaseObject;
use rabbit\helper\ArrayHelper;
use rabbit\redis\Redis;

/**
 * Interface AbstractPlugin
 * @package Rabbit\Data\Pipeline
 */
abstract class AbstractPlugin extends BaseObject implements InitInterface
{
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
    public $opt = [];
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
    protected $schedulerName = 'schedule';

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
            App::info("Road from $this->key to $output", 'Data');
            getDI($this->schedulerName)->send($this->taskName, $output, $this->task_id, $data, $workerId ?? $transfer, $this->opt);
        }
    }
}
