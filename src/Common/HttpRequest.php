<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Rabbit\Base\App;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\FileHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\HttpClient\Client;
use Swlib\Saber\Request;
use Throwable;

/**
 * Class HttpRequest
 * @package Rabbit\Data\Pipeline\Common
 */
class HttpRequest extends AbstractPlugin
{
    protected bool $usePool = false;
    protected int $timeout = 60;
    protected ?int $throttleTime;
    protected ?string $retry;
    protected string $format;
    protected bool $download;
    protected ?string $checkResponseFunc;
    protected string $driver = 'saber';
    protected Client $client;
    protected ?bool $isLog;

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $this->driver,
            $this->usePool,
            $this->timeout,
            $this->retry,
            $this->format,
            $this->download,
            $this->throttleTime,
            $this->checkResponseFunc,
            $this->isLog,
        ] = ArrayHelper::getValueByArray($this->config, [
            'driver',
            'usePool',
            'timeout',
            'retry',
            'format',
            'download',
            'throttleTime',
            'checkResponseFunc',
            'isLog'
        ], [
            'saber',
            true,
            60,
            null,
            'string',
            false,
            null,
            null,
            true
        ]);
        if ($this->retry && !is_callable($this->retry)) {
            throw new InvalidConfigException("The retry must be callable");
        }
        $this->client = new Client();
    }

    /**
     * @param Message $msg
     * @throws Exception
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        $path = ArrayHelper::remove($msg->data, 'download_dir');
        $throttleTime = ArrayHelper::remove($msg->data, 'throttleTime', $this->throttleTime);
        $request_id = uniqid();
        if ($this->download && $path) {
            FileHelper::createDirectory(dirname($path), 777);
            $msg->data += ['download_dir' => $path];
        }

        $options = [
            'timeout' => $msg->opt['requestTimeOut'] ?? $this->timeout
        ];
        if ($this->driver === 'saber') {
            $options = array_merge([
                'use_pool' => $this->usePool
            ], $options, $msg->data);
            if ($this->isLog) {
                $options = ArrayHelper::merge($options, [
                    "before" => [function (RequestInterface $request) use ($request_id) {
                        $uri = $request->getUri();
                        App::info(
                            sprintf(
                                "Request %s %s %s with body [%s]",
                                $request_id,
                                $request->getMethod(),
                                $uri->getScheme() . "://" . $uri->getHost() . $uri->getPath(),
                                (string)$request->getBody()
                            ),
                            "http"
                        );
                    }],
                    'after' => [function (ResponseInterface $response) use ($request_id) {
                        App::info("Request $request_id finish");
                    }]
                ]);
            }
            if ($this->retry) {
                $options['retry'] = function (Request $request) use ($throttleTime) {
                    return call_user_func($this->retry, $request, $throttleTime);
                };
            }
        }

        $response = $this->client->request($options, $this->driver);
        if (!$this->download) {
            $format = $this->format;
            if (method_exists($response, $format)) {
                $msg->data = $response->$format();
            } else {
                $msg->data = (string)$response->getBody();
            }
        } else {
            $msg->data = $path;
        }
        if (is_callable($this->checkResponseFunc)) {
            call_user_func($this->checkResponseFunc, $msg);
        }
        $this->sink($msg);
    }
}
