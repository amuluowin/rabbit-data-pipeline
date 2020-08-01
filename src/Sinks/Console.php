<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DOMException;
use Rabbit\Base\Exception\InvalidCallException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\JsonHelper;
use Rabbit\Base\Helper\VarDumper;
use Rabbit\Base\Helper\XmlHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;

/**
 * Class Console
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Console extends AbstractPlugin
{
    protected string $encoding;
    protected string $method;

    public function init(): void
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

    /**
     * @param Message $msg
     * @throws DOMException
     */
    public function run(Message $msg): void
    {
        switch ($this->encoding) {
            case 'json':
                if ($this->method === 'echo') {
                    echo JsonHelper::encode(VarDumper::getDumper()->dumpAsString($msg->data)) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func($this->method, JsonHelper::encode(VarDumper::getDumper()->dumpAsString($msg->data)));
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'html':
                if ($this->method === 'echo') {
                    echo htmlspecialchars_decode(VarDumper::getDumper()->dumpAsString($msg->data)) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func(
                        $this->method,
                        htmlspecialchars_decode(VarDumper::getDumper()->dumpAsString($msg->data))
                    );
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'xml':
                if ($this->method === 'echo') {
                    echo XmlHelper::format((array)$msg->data) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func(
                        $this->method,
                        XmlHelper::format((array)$msg->data)
                    );
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
                break;
            case 'text':
            default:
                if ($this->method === 'echo') {
                    echo VarDumper::getDumper()->dumpAsString($msg->data) . PHP_EOL;
                } elseif (is_callable($this->method)) {
                    call_user_func($this->method, VarDumper::getDumper()->dumpAsString($msg->data));
                } else {
                    throw new InvalidCallException("$this->method not callable!");
                }
        }
    }
}
