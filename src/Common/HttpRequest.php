<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use rabbit\App;
use rabbit\core\Exception;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\FileHelper;
use Swlib\Http\Exception\RequestException;
use Swlib\Http\Exception\TransferException;
use Swlib\Saber\Request;
use Swlib\Saber\Response;
use Swlib\SaberGM;

/**
 * Class HttpRequest
 * @package Rabbit\Data\Pipeline\Common
 */
class HttpRequest extends AbstractPlugin
{
    /** @var bool */
    protected $usePool = true;
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

    /**
     * @return mixed|void
     * @throws InvalidConfigException
     */
    public function init()
    {
        parent::init();
        [
            $this->usePool,
            $this->timeout,
            $this->error,
            $this->retry,
            $this->format,
            $this->download,
            $this->throttleTime,
            $this->checkResponseFunc,
        ] = ArrayHelper::getValueByArray($this->config, [
            'usePool',
            'timeout',
            'error',
            'retry',
            'format',
            'download',
            'throttleTime',
            'checkResponseFunc'
        ], null, [
            true,
            60,
            null,
            null,
            'string',
            false,
            null,
            null
        ]);
//        if ($this->error && !is_callable($this->error)) {
//            throw new InvalidConfigException("The error must be callable");
//        }
        if ($this->retry && !is_callable($this->retry)) {
            throw new InvalidConfigException("The retry must be callable");
        }
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
            'use_pool' => $this->usePool,
            'timeout' => ArrayHelper::getValue($this->opt, 'requestTimeOut', $this->timeout),
            "before" => function (Request $request) use ($request_id) {
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
            'after' => function (Response $response) use ($request_id) {
                App::info("Request $request_id finish");
            }
        ];
        if ($this->retry) {
            $options['retry'] = function (Request $request) {
                return call_user_func($this->retry, $request, $this->throttleTime === null ? $throttleTime : $this->throttleTime);
            };
        }
        $response = SaberGM::request(array_merge($this->input, $options));
        if (!$this->download) {
            $format = 'getParsed' . $this->format;
            if (method_exists($response, $format)) {
                $outPutData = $response->$format();
            } else {
                $outPutData = (string)$response->getBody();
            }
        } else {
            $outPutData = $path;
        }
        if(is_callable($this->checkResponseFunc)){
            call_user_func_array($this->checkResponseFunc, [&$outPutData]);
        }
        $this->output($outPutData);
    }
}
