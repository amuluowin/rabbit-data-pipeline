<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Rabbit\Server\Server;
use Rabbit\Server\ServerHelper;
use Rabbit\Base\Exception\InvalidConfigException;

class WorkerSender implements ISender
{
    /**
     * @param string $address
     * @param string $target
     * @param AbstractPlugin $pre
     * @param $data
     * @return array|null
     * @throws Throwable
     */
    public function send(string $target, Message $msg, string $address = null, float $wait = 0): ?array
    {
        $tmp = ['scheduler->next', [$msg, $target, $wait]];
        $server = ServerHelper::getServer();
        if (!$server instanceof Server) {
            throw new InvalidConfigException("only use for swoole_server");
        }
        $server->pipeHandler->sendMessage($tmp, (int)$address, $wait);
        return null;
    }
}
