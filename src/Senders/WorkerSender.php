<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Senders;

use Rabbit\Base\App;
use Rabbit\Server\Server;
use Rabbit\Server\ServerHelper;
use Rabbit\Server\CommonHandler;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Data\Pipeline\Message;

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
        if (null === $server = ServerHelper::getServer()) {
            App::warning("Not running in server, use local process");
            $msg !== null && CommonHandler::handler($this, $tmp);
            return null;
        }
        if (!$server instanceof Server) {
            throw new InvalidConfigException("only use for swoole_server");
        }
        $server->pipeHandler->sendMessage($tmp, (int)$address, $wait);
        return null;
    }
}
