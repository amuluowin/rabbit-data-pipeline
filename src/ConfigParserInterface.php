<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

/**
 * Interface ConfigParserInterface
 * @package Rabbit\Data\Pipeline
 */
interface ConfigParserInterface
{
    /**
     * @return array
     */
    public function parse(): array;

    public function parseTask(string $task): array;
}
