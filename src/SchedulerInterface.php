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
     * @param AbstractPlugin $pre
     * @param string $key
     * @param $data
     * @param bool $transfer
     */
    public function send(AbstractPlugin $pre, string $key, &$data, bool $transfer): void;

    /**
     * @param string|null $key
     * @return bool
     */
    public function getLock(string $key = null): bool;

    /**
     * @param array $opt
     * @param string $taskName
     */
    public function deleteAllLock(string $taskName, array $opt = []): void;

    /**
     * @param string|null $key
     * @param string $taskName
     * @return int
     */
    public function deleteLock(string $taskName, string $key = null): int;
}