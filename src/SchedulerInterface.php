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
     * @param array $params
     */
    public function run(string $key = null, array $params = []): void;

    /**
     * @param string $taskName
     * @param string $key
     * @param string|null $task_id
     * @param $data
     * @param int|null $transfer
     * @param array $opt
     * @param array $request
     * @param bool $wait
     */
    public function send(string $taskName, string $key, ?string $task_id, &$data, bool $transfer, array $opt = [], array $request = [], bool $wait = false): void;

    /**
     * @param string|null $key
     * @return bool
     */
    public function getLock(string $key = null): bool;

    /**
     * @param array $opt
     * @param string $taskName
     */
    public function deleteAllLock(array $opt = [], string $taskName): void;

    /**
     * @param string|null $key
     * @param string $taskName
     * @return int
     */
    public function deleteLock(string $key = null, string $taskName): int;
}