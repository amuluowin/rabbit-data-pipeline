<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

interface SchedulerInterface
{
    public function getConfig(): array;

    public function getTarget(string $taskName, string $name): AbstractPlugin;

    public function process(string $task, array $params = []): void;

    public function run(string $key, string $target = null, array $params = []): array;

    public function next(Message $pre, string $key, float $wait = 0): void;
}
