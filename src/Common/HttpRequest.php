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
use Rabbit\HttpClient\Client;
use Swlib\Saber\Request;
use Throwable;

/**
 * Class HttpRequest
 * @package Rabbit\Data\Pipeline\Common
 */
class HttpRequest extends AbstractPlugin
{
    /** @var bool */
    protected bool $usePool = false;
    /** @var int */
    protected int $timeout = 60;
    /** @var int */
    protected ?int $throttleTime;
    /** @var string */
    protected ?string $error;
    /** @var string */
    protected ?string $retry;
    /** @var string */
    protected string $format;
    /** @var bool */
    protected bool $download;
    /** @var string */
    protected ?string $checkResponseFunc;
    /** @var string */
    protected string $driver = 'saber';
    /** @var Client */
    protected Client $client;

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init()
    {
        parent::init();
        [
            $this->driver,
            $this->usePool,
            $this->timeout,
            $this->error,
            $this->retry,
            $this->format,
            $this->download,
            $this->throttleTime,
            $this->checkResponseFunc,
        ] = ArrayHelper::getValueByArray($this->config, [
            'driver',
            'usePool',
            'timeout',
            'error',
            'retry',
            'format',
            'download',
            'throttleTime',
            'checkResponseFunc'
        ], [
            'saber',
            true,
            60,
            null,
            null,
            'string',
            false,
            null,
            null
        ]);
        if ($this->retry && !is_callable($this->retry)) {
            throw new InvalidConfigException("The retry must be callable");
        }
        $this->client = new Client();
    }

    /**
     * @throws Throwable
     * @throws Exception
     */
    public function run(): void
    {
        $path = ArrayHelper::remove($this->input, 'download_dir');
        $throttleTime = ArrayHelper::remove($this->input, 'throttleTime', 0);
        $throttleTime = $throttleTime > 0 ? $throttleTime : $this->throttleTime;
        $request_id = uniqid();
        if ($this->download && $path) {
            FileHelper::createDirectory(dirname($path), 777);
            $this->input += ['download_dir' => $path];
        }

        $options = [
            'timeout' => ArrayHelper::getValue($this->opt, 'requestTimeOut', $this->timeout),
        ];
        if ($this->driver === 'saber') {
            $options = array_merge([
                'use_pool' => $this->usePool,
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
            ], $options, $this->input);
            if ($this->retry) {
                $options['retry'] = function (Request $request, $throttleTime) {
                    return call_user_func($this->retry, $request, $this->throttleTime === null ? $throttleTime : $this->throttleTime);
                };
            }
        }

        $response = $this->client->request($options, $this->driver);
        if (!$this->download) {
            $format = $this->format;
            if (method_exists($response, $format)) {
                $outPutData = $response->$format();
            } else {
                $outPutData = (string)$response->getBody();
            }
        } else {
            $outPutData = $path;
        }
        if (is_callable($this->checkResponseFunc)) {
            call_user_func_array($this->checkResponseFunc, [&$outPutData]);
        }
        $this->output($outPutData);
    }
}
