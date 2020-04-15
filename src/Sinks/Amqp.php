<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Amqp\Connection;
use Rabbit\Amqp\Manager;
use rabbit\compool\BaseCompool;
use rabbit\compool\ComPoolProperties;
use rabbit\core\ObjectFactory;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\helper\ArrayHelper;

/**
 * Class Amqp
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Amqp extends AbstractSingletonPlugin
{
    /** @var Connection */
    protected $conn;
    /** @var array */
    protected $properties = [
        'content_type' => 'text/plain',
        'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT
    ];

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
            $queue,
            $exchange,
            $count,
            $connParams,
            $queueDeclare,
            $exchangeDeclare,
            $this->properties
        ] = ArrayHelper::getValueByArray($this->config, [
            'queue',
            'exchange',
            'count',
            'connParams',
            'queueDeclare',
            'exchangeDeclare',
            'properties'
        ], null, [
            '',
            '',
            '',
            5,
            [],
            [],
            [],
            $this->properties
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
        $this->conn->basic_publish(new AMQPMessage($this->input, $this->properties));
    }
}