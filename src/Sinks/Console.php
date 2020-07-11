<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Base\Exception\InvalidCallException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\JsonHelper;
use Rabbit\Base\Helper\VarDumper;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class Console
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Console extends AbstractPlugin
{
    /** @var string */
    protected string $encoding;
    /** @var string */
    protected string $method;

    public function init()
    {
        parent::init();
        [$this->encoding, $this->method] = ArrayHelper::getValueByArray($this->config, [
            'encoding',
            'method'
        ], [
            'text',
            'echo'
        ]);
    }

    public function run(): void
    {
        switch ($this->encoding) {
            case 'json':
                if ($this->method === 'echo') {
                    echo JsonHelper::encode(VarDumper::getDumper()->dumpAsString($this->input)) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func($this->method, JsonHelper::encode(VarDumper::getDumper()->dumpAsString($this->input)));
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'html':
                if ($this->method === 'echo') {
                    echo htmlspecialchars_decode(VarDumper::getDumper()->dumpAsString($this->input)) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func(
                        $this->method,
                        htmlspecialchars_decode(VarDumper::getDumper()->dumpAsString($this->input))
                    );
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'xml':
                if ($this->method === 'echo') {
                    echo XmlFormatHelper::format(VarDumper::getDumper()->dumpAsString($this->input)) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func(
                        $this->method,
                        XmlFormatHelper::format(VarDumper::getDumper()->dumpAsString($this->input))
                    );
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'text':
            default:
                if ($this->method === 'echo') {
                    echo VarDumper::getDumper()->dumpAsString($this->input) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func($this->method, VarDumper::getDumper()->dumpAsString($this->input));
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
        }
    }
}
