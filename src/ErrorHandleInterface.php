<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Throwable;

/**
 * Interface ErrorHandleInterface
 * @package Rabbit\Data\Pipeline
 */
interface ErrorHandleInterface
{
    /**
     * @param AbstractPlugin $plugin
     * @param Throwable $throwable
     * @return mixed
     */
    public static function handle(AbstractPlugin $plugin, Throwable $throwable);
}