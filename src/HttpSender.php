<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use rabbit\App;
use rabbit\httpclient\Client;

/**
 * Class HttpSender
 * @package Rabbit\Data\Pipeline
 */
class HttpSender implements ISender
{
    /** @var Client */
    protected $client;
    /** @var string */
    protected $route = '/api/schedule/run';

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
     * @throws \Exception
     */
    public function send(string $address, string $target, AbstractPlugin $pre, &$data): ?array
    {
        $response = $this->client->request([
            'uri' => $address . $this->route,
            'method' => 'POST',
            'json' => [
                'key' => $pre->taskName,
                'target' => $target,
                'data' => [
                    'taskId' => &$pre->taskId,
                    'input' => &$data,
                    'opt' => &$pre->opt,
                    'request' => &$pre->request
                ]
            ]
        ]);
        if ($response->getStatusCode() === 200) {
            App::info("send $pre->taskName $target success");
            return $response->jsonArray();
        }
        App::error("send $pre->taskName $target failed " . (string)$response->getBody());
        return null;
    }

}