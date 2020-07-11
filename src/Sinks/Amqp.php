<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use PhpAmqpLib\Message\AMQPMessage;
use Rabbit\Amqp\Connection;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use Rabbit\Pool\BaseManager;
use Rabbit\Pool\BasePool;
use Rabbit\Pool\BasePoolProperties;
use Throwable;

/**
 * Class Amqp
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Amqp extends AbstractSingletonPlugin
{
    /** @var string */
    protected string $name;
    /** @var array */
    protected array $properties = [
        'content_type' => 'text/plain',
        'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT
    ];

    /**
     * @return mixed|void
     * @throws Throwable
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
            $connParams,
            $queueDeclare,
            $exchangeDeclare,
            $this->properties
        ] = ArrayHelper::getValueByArray($this->config, [
            'queue',
            'exchange',
            'connParams',
            'queueDeclare',
            'exchangeDeclare',
            'properties'
        ], [
            '',
            '',
            '',
            [],
            [],
            [],
            $this->properties
        ]);
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

    /**
     * @throws Throwable
     */
    public function run()
    {
        /** @var BaseManager $amqp */
        $amqp = getDI('amqp');
        $conn = $amqp->get($this->name);
        $conn->basic_publish(new AMQPMessage($this->input, $this->properties));
    }
}