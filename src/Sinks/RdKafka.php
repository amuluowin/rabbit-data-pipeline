<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use Rabbit\Rdkafka\KafkaManager;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\Topic;
use Throwable;

/**
 * Class RdKafka
 * @package Rabbit\Data\Pipeline\Sinks
 */
class RdKafka extends AbstractSingletonPlugin
{
    /** @var ProducerTopic */
    protected ?ProducerTopic $topic;
    /** @var Producer */
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
        $manager = getDI('kafka');
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

    public function run()
    {
        $this->topic->produce(RD_KAFKA_PARTITION_UA, 0, $this->getInput());
        $this->producer->poll(0);
        $this->producer->flush(1000);
    }
}