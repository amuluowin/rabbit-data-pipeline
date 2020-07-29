<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Rabbit\Base\App;
use Rabbit\HttpClient\Client;
use Throwable;

/**
 * Class HttpSender
 * @package Rabbit\Data\Pipeline
 */
class HttpSender implements ISender
{
    /** @var Client */
    protected Client $client;
    /** @var string */
    protected string $route = '/api/schedule/run';

    /**
     * HttpSender constructor.
     */
    public function __construct()
    {
        $this->client = new Client(['usePool' => true]);
    }

    /**
     * @param string $address
     * @param string $target
     * @param AbstractPlugin $pre
     * @param $data
     * @return array|null
     * @throws Throwable
     */
    public function send(string $address, string $target, Message $msg): ?array
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
            ]
        ]);
        if ($response->getStatusCode() === 200) {
            App::info("send $msg->taskName $target success");
            return $response->jsonArray();
        }
        App::error("send $msg->taskName $target failed " . (string)$response->getBody());
        return null;
    }

}