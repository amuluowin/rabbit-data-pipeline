<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Senders;

use Rabbit\Data\Pipeline\Message;

class ProcessSender implements ISender
{
    /**
     * @author Albert <63851587@qq.com>
     * @param string $target
     * @param Message $msg
     * @param string $address
     * @param float $wait
     * @return array|null
     */
    public function send(string $target, Message $msg, string $address = null, float $wait = 0): ?array
    {
        $socketHandle = getDI('socketHandle');
        $tmp = ['scheduler->next', [$msg, $target, $wait]];
        $socketHandle->send($tmp, (int)$address, $wait);
        return null;
    }
}
