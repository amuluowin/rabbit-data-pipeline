<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

/**
 * Interface ISender
 * @package Rabbit\Data\Pipeline
 */
interface ISender
{
    /**
     * @param string $address
     * @param string $target
     * @param AbstractPlugin $pre
     * @param $data
     * @return array|null
     */
    public function send(string $address, string $target, AbstractPlugin $pre, &$data): ?array;
}