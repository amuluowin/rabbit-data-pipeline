<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use ErrorException;
use PhpAmqpLib\Message\AMQPMessage;
use Rabbit\Amqp\Connection;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Pool\BaseManager;
use Rabbit\Pool\BasePool;
use Rabbit\Pool\BasePoolProperties;
use Throwable;

/**
 * Class Amqp
 * @package Rabbit\Data\Pipeline\Sources
 */
class Amqp extends AbstractPlugin
{
    protected Connection $conn;
    protected string $consumerTag;

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $name,
            $this->consumerTag,
            $queue,
            $exchange,
            $connParams,
            $queueDeclare,
            $exchangeDeclare,
        ] = ArrayHelper::getValueByArray($this->config, [
            'name',
            'consumerTag',
            'queue',
            'exchange',
            'connParams',
            'queueDeclare',
            'exchangeDeclare'
        ], [
            null,
            '',
            '',
            '',
            [],
            [],
            []
        ]);
        if (!$name) {
            $name = uniqid();
            /** @var BaseManager $amqp */
            $amqp = getDI('amqp');
            $amqp->add([
                $name => create([
                    'class' => BasePool::class,
                    'poolConfig' => create([
                        'class' => BasePoolProperties::class,
                        'config' => [
                            'queue' => $queue,
                            'exchange' => $exchange,
                            'connParams' => $connParams,
                            'queueDeclare' => $queueDeclare,
                            'exchangeDeclare' => $exchangeDeclare
                        ]
                    ]),
                    'objClass' => Connection::class
                ])
            ]);
        }
        $pool = $amqp->get($name);
        $this->conn = $pool->get();
        $pool->sub();
    }

    /**
     * @param Message $msg
     * @throws ErrorException
     */
    public function run(Message $msg): void
    {
        $this->conn->consume(
            $this->consumerTag,
            false,
            false,
            false,
            false,
            function (AMQPMessage $message) use ($msg): void {
                $tmp = clone $msg;
                $tmp->data = $message->body;
                $this->sink($tmp);
            }
        );
    }
}
