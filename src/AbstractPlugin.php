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
use rabbit\redis\Redis;

/**
 * Interface AbstractPlugin
 * @package Rabbit\Data\Pipeline
 */
abstract class AbstractPlugin extends BaseObject implements InitInterface
{
    /** @var string */
    protected $taskName;
    /** @var string */
    protected $key;
    /** @var array */
    protected $config = [];
    /** @var array */
    protected $output = [];
    /** @var bool */
    protected $start = false;
    /** @var string */
    protected $logKey = 'Plugin';
    /** @var Redis */
    protected $redis;
    /** @var int */
    protected $lockEx = 0;
    /** @var CacheInterface */
    protected $cache;
    /** @var string */
    const CACHE_KEY = 'cache';

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
        [
            $cache
        ] = ArrayHelper::getValueByArray($this->config, [
            self::CACHE_KEY
        ], null, [
            'memory'
        ]);
        $this->cache = getDI(self::CACHE_KEY)->getDriver($cache);
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
        if ($key || $key = $this->getTaskId()) {
            return (bool)$this->redis->set($key, true, ['nx', 'ex' => $this->lockEx]);
        }
        return true;
    }

    /**
     * @param string $lockKey
     * @return bool
     */
    public function deleteLock(string $key = null): int
    {
        return $this->redis->del($key ?? $this->getTaskId());
    }

    /**
     * @param string $task_id
     */
    public function setTaskId(string $task_id): void
    {
        Context::set($this->taskName, $task_id);
    }

    /**
     * @return string
     */
    public function getTaskId(): ?string
    {
        return Context::get($this->taskName);
    }

    /**
     * @param array $input
     * @param string $key
     * @param null $default
     * @return mixed|null
     */
    public function getFromInput(array $input, string $key, $default = null)
    {
        return ArrayHelper::getValue($input, $key, $default);
    }

    /**
     * @param array $data
     */
    public function process(array &$data): void
    {
        [$task_id, &$data] = $data;
        $this->setTaskId($task_id);
        $this->input($data);
    }

    /**
     * @param $input
     */
    abstract public function input(&$input = null);

    /**
     * @param $data
     * @throws Exception
     */
    public function output(&$data): void
    {
        $task_id = $this->getTaskId();
        foreach ($this->output as $output => $process) {
            App::info("Road from $this->key to $output", 'Data');
            getDI('scheduler')->send($this->taskName, $output, $task_id, $data, $process);
        }
    }
}
