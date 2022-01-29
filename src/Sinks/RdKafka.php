<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Rdkafka\KafkaManager;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use Throwable;

/**
 * Class RdKafka
 * @package Rabbit\Data\Pipeline\Sinks
 */
class RdKafka extends AbstractPlugin
{
    protected ?ProducerTopic $topic;
    protected ?Producer $producer;

    /**
     * @return mixed|void
     * @throws InvalidConfigException
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $dsn,
            $topic,
            $options,
            $topicSet,
        ] = ArrayHelper::getValueByArray($this->config, [
            'dsn',
            'topic',
            'options',
            'topicSet'
        ], [
            'options' => [],
            'topicSet' => []
        ]);
        if (empty($dsn) || empty($topic)) {
            throw new InvalidConfigException("dsn & topic must be set!");
        }
        $name = md5($dsn);
        /** @var KafkaManager $manager */
        $manager = service('kafka');
        $manager->add([
            $name => [
                'type' => 'producer',
                'dsn' => $dsn,
                'set' => $options
            ]
        ]);
        $manager->init();
        $this->producer = $manager->getProducer($name);
        $this->topic = $manager->getProducerTopic($name, $topic, $topicSet);
    }

    public function run(Message $msg): void
    {
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $msg->data);
        $this->producer->poll(0);
        $this->producer->flush(1000);
    }
}