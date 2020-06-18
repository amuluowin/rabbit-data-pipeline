<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use PhpAmqpLib\Message\AMQPMessage;
use Rabbit\Amqp\Connection;
use Rabbit\Amqp\Manager;
use rabbit\core\ObjectFactory;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\helper\ArrayHelper;
use rabbit\pool\BasePool;
use rabbit\pool\BasePoolProperties;

/**
 * Class Amqp
 * @package Rabbit\Data\Pipeline\Sources
 */
class Amqp extends AbstractSingletonPlugin
{
    /** @var Connection */
    protected $conn;
    /** @var string */
    protected $consumerTag;

    /**
     * @return mixed|void
     * @throws \Exception
     */
    public function init()
    {
        parent::init();
        array_walk($this->output, function (&$value) {
            $value = false;
        });
        [
            $this->consumerTag,
            $queue,
            $exchange,
            $count,
            $connParams,
            $queueDeclare,
            $exchangeDeclare,
        ] = ArrayHelper::getValueByArray($this->config, [
            'consumerTag',
            'queue',
            'exchange',
            'count',
            'connParams',
            'queueDeclare',
            'exchangeDeclare'
        ], null, [
            '',
            '',
            '',
            5,
            [],
            [],
            []
        ]);
        $name = uniqid();
        /** @var Manager $amqp */
        $amqp = getDI('amqp');
        $amqp->add([
            $name => ObjectFactory::createObject([
                'class' => BasePool::class,
                'poolConfig' => ObjectFactory::createObject([
                    'class' => BasePoolProperties::class,
                    'config' => [
                        'queue' => $queue,
                        'exchange' => $exchange,
                        'connParams' => $connParams,
                        'queueDeclare' => $queueDeclare,
                        'exchangeDeclare' => $exchangeDeclare
                    ]
                ]),
                'objclass' => Connection::class
            ])
        ]);
        $this->conn = $amqp->get($name)->get();
    }

    public function run()
    {
        $this->conn->consume($this->consumerTag, false, false, false, false, function (AMQPMessage $message) {
            $this->output($message->body);
        });
    }
}