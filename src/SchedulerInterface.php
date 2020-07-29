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
     * @param string|null $key
     * @param string|null $target
     * @param array $params
     */
    public function run(string $key = null, string $target = null, array $params = []): void;

    /**
     * @param Message $pre
     * @param string $key
     * @param bool $transfer
     */
    public function send(Message $pre, string $key, bool $transfer): void;
}