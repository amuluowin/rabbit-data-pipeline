<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Senders;

use Rabbit\Base\App;
use Rabbit\Server\Server;
use Rabbit\Server\ServerHelper;
use Rabbit\Server\CommonHandler;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Server\IPCMessage;

class WorkerSender implements ISender
{
    public function send(string $target, Message $msg, string $address, float $wait = 0): ?array
    {
        $ipc = new IPCMessage([
            'data' => ['scheduler->next', [$msg, $target, $wait]],
            'wait' => $wait,
            'to' => $address === null ? -1 : (int)$address
        ]);
        if (null === $server = ServerHelper::getServer()) {
            App::warning("Not running in server, use local process");
            $ipc = create(CommonHandler::class)->handler($this, $ipc);
        } elseif (!$server instanceof Server) {
            throw new InvalidConfigException("only use for swoole_server");
        } else {
            $ipc = $server->pipeHandler->sendMessage($ipc);
        }
        if ($ipc->error !== null) {
            throw new $ipc->error;
        }
        return $ipc->data;
    }
}
