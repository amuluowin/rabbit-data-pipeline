<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Senders;

use Rabbit\Base\App;
use Rabbit\Data\Pipeline\Message;
use Rabbit\HttpClient\Client;

class HttpSender implements ISender
{
    protected Client $client;
    protected string $route = '/schedule/run';

    public function __construct()
    {
        $this->client = new Client(['usePool' => true]);
    }

    public function send(string $target, Message $msg, string $address, float $wait = 0)
    {
        $response = $this->client->request([
            'uri' => $address . $this->route,
            'method' => 'POST',
            'json' => [
                'key' => $msg->taskName,
                'target' => $target,
                'data' => [
                    'taskId' => $msg->taskId,
                    'data' => &$msg->data,
                    'opt' => $msg->opt,
                    'request' => $msg->request
                ]
            ],
            'timeout' => $wait > 0 ? $wait : -1
        ]);
        if ($response->getStatusCode() === 200) {
            App::debug("send $msg->taskName $target success");
            return $response->jsonArray();
        }
        App::error("send $msg->taskName $target failed " . (string)$response->getBody());
        return null;
    }
}
