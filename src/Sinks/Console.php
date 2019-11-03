<?php


namespace Rabbit\Data\Pipeline\Sinks;

use common\Helpers\XmlFormatHelper;
use rabbit\exception\InvalidCallException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\JsonHelper;
use rabbit\helper\VarDumper;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class Console
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Console extends AbstractPlugin
{
    /** @var string */
    protected $encoding = 'text';
    /** @var string */
    protected $method = 'echo';

    public function init()
    {
        parent::init();
        [$this->encoding, $this->method] = ArrayHelper::getValueByArray($this->config, [
            'encoding',
            'method'
        ], null, [
            'text',
            'echo'
        ]);
    }

    /**
     * @param null $input
     * @param array $opt
     */
    public function input(&$input = null, &$opt = []): void
    {
        switch ($this->encoding) {
            case 'json':
                if ($this->method === 'echo') {
                    echo JsonHelper::encode(VarDumper::getDumper()->dumpAsString($input)) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func($this->method, JsonHelper::encode(VarDumper::getDumper()->dumpAsString($input)));
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'html':
                if ($this->method === 'echo') {
                    echo htmlspecialchars_decode(VarDumper::getDumper()->dumpAsString($input)) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func(
                        $this->method,
                        htmlspecialchars_decode(VarDumper::getDumper()->dumpAsString($input))
                    );
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'xml':
                if ($this->method === 'echo') {
                    echo XmlFormatHelper::format(VarDumper::getDumper()->dumpAsString($input)) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func(
                        $this->method,
                        XmlFormatHelper::format(VarDumper::getDumper()->dumpAsString($input))
                    );
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'text':
            default:
                if ($this->method === 'echo') {
                    echo VarDumper::getDumper()->dumpAsString($input) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func($this->method, VarDumper::getDumper()->dumpAsString($input));
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
        }
    }
}
