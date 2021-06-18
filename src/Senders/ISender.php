<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Senders;

use Rabbit\Data\Pipeline\Message;

interface ISender
{
    public function send(string $target, Message $msg, string $address, float $wait = 0);
}
