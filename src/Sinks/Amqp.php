<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Amqp\Connection;
use Rabbit\Amqp\Manager;
use rabbit\core\ObjectFactory;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\helper\ArrayHelper;
use rabbit\pool\BasePool;
use rabbit\pool\BasePoolProperties;

/**
 * Class Amqp
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Amqp extends AbstractSingletonPlugin
{
    /** @var string */
    protected $name;
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
    }

    public function run()
    {
        /** @var Manager $amqp */
        $amqp = getDI('amqp');
        $conn = $amqp->get($this->name);
        $conn->basic_publish(new AMQPMessage($this->input, $this->properties));
    }
}