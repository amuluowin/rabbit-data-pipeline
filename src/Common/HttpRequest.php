<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use rabbit\App;
use rabbit\core\Exception;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\FileHelper;
use rabbit\httpclient\Client;
use Swlib\Saber\Request;

/**
 * Class HttpRequest
 * @package Rabbit\Data\Pipeline\Common
 */
class HttpRequest extends AbstractSingletonPlugin
{
    /** @var bool */
    protected $usePool = false;
    /** @var int */
    protected $timeout = 60;
    /** @var int */
    protected $throttleTime;
    /** @var string */
    protected $error;
    /** @var string */
    protected $retry;
    /** @var string */
    protected $format;
    /** @var bool */
    protected $download;
    /** @var string */
    protected $checkResponseFunc;
    /** @var string */
    protected $driver = 'saber';
    /** @var Client */
    protected $client;

    /**
     * @return mixed|void
     * @throws InvalidConfigException
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
        ], null, [
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
     * @throws Exception
     */
    public function run(): void
    {
        $path = ArrayHelper::remove($this->input, 'download_dir');
        $throttleTime = ArrayHelper::remove($this->input, 'throttleTime', 0);
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
                "before" => function (RequestInterface $request) use ($request_id) {
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
                },
                'after' => function (ResponseInterface $response) use ($request_id) {
                    App::info("Request $request_id finish");
                }
            ], $options, $this->input);
            if ($this->retry) {
                $options['retry'] = function (Request $request) {
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
