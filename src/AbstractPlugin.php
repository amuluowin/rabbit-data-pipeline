<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Exception;
use rabbit\App;
use rabbit\contract\InitInterface;
use rabbit\core\BaseObject;
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

    /**
     * AbstractPlugin constructor.
     * @param array $config
     * @throws Exception
     */
    public function __construct(array $config)
    {
        $this->config = $config;
    }

    public function init()
    {
        $this->redis = getDI('redis');
    }

    /**
     * @return bool
     */
    public function getStart(): bool
    {
        return $this->start;
    }

    /**
     * @param $input
     */
    abstract public function input(&$input = null): void;

    /**
     * @param $data
     * @throws Exception
     */
    public function output(&$data): void
    {
        foreach ($this->output as $output => $process) {
            App::info("Road from $this->key to $output", 'Data');
            getDI('scheduler')->send($this->taskName, $output, $data, $process);
        }
    }
}
