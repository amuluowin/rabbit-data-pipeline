<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use PhpAmqpLib\Message\AMQPMessage;
use Rabbit\Amqp\Connection;
use Rabbit\Amqp\Manager;
use rabbit\compool\BaseCompool;
use rabbit\compool\ComPoolProperties;
use rabbit\core\ObjectFactory;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\helper\ArrayHelper;

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
        $amqp->addConnection([
            $name => ObjectFactory::createObject([
                'class' => BaseCompool::class,
                'poolConfig' => ObjectFactory::createObject([
                    'class' => ComPoolProperties::class,
                    'config' => [
                        'queue' => $queue,
                        'exchange' => $exchange,
                        'connParams' => $connParams,
                        'queueDeclare' => $queueDeclare,
                        'exchangeDeclare' => $exchangeDeclare
                    ]
                ]),
                'comClass' => Connection::class
            ])
        ]);
        $this->conn = $amqp->getConnection($name);
    }

    public function run()
    {
        $this->conn->consume($this->consumerTag, false, false, false, false, function (AMQPMessage $message) {
            $this->output($message->body);
        });
    }
}