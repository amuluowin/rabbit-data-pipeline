<?php
declare (strict_types = 1);

namespace Rabbit\Data\Pipeline;

/**
 * Interface ISender
 * @package Rabbit\Data\Pipeline
 */
interface ISender
{
    /**
     * @author Albert <63851587@qq.com>
     * @param string $target
     * @param Message $msg
     * @param string $address
     * @return array|null
     */
    public function send(string $target, Message $msg, string $address = null): ?array;
}
