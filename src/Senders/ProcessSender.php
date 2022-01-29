<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Senders;

use Rabbit\Data\Pipeline\Message;
use Rabbit\Server\IPCMessage;

class ProcessSender implements ISender
{
    public function send(string $target, Message $msg, string $address, float $wait = 0): ?array
    {
        $socketHandle = service('socketHandle');
        $ipc = new IPCMessage([
            'data' => ['scheduler->next', [$msg, $target, $wait]],
            'wait' => $wait,
            'to' => $address === null ? -1 : (int)$address
        ]);
        $ipc = $socketHandle->send($ipc);
        if ($ipc->error !== null) {
            throw new $ipc->error;
        }
        return $ipc->data;
    }
}
