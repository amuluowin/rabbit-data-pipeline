<?php


namespace Rabbit\Data\Pipeline;

/**
 * Interface ErrorHandleInterface
 * @package Rabbit\Data\Pipeline
 */
interface ErrorHandleInterface
{
    /**
     * @param AbstractPlugin $plugin
     * @param \Throwable $throwable
     * @return mixed
     */
    public static function handle(AbstractPlugin $plugin,\Throwable $throwable);
}