<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

/**
 * Interface SchedulerInterface
 * @package Rabbit\Data\Pipeline
 */
interface SchedulerInterface
{
    /**
     * @author Albert <63851587@qq.com>
     * @return array
     */
    public function getConfig(): array;
    /**
     * @param string $taskName
     * @param string $name
     * @return AbstractPlugin|null
     */
    public function getTarget(string $taskName, string $name): AbstractPlugin;

    /**
     * @param string $task
     * @param array $params
     */
    public function process(string $task, array $params = []): void;

    /**
     * @author Albert <63851587@qq.com>
     * @param string $key
     * @param string $target
     * @param array $params
     * @return array
     */
    public function run(string $key, string $target = null, array $params = []): array;

    /**
     * @param Message $pre
     * @param string $key
     */
    public function next(Message $pre, string $key, float $wait = 0): void;
}
