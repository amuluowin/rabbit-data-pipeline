<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use Rabbit\Rdkafka\KafkaManager;
use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Throwable;

/**
 * Class RdKafka
 * @package Rabbit\Data\Pipeline\Sources
 */
class RdKafka extends AbstractSingletonPlugin
{
    /** @var KafkaConsumer */
    protected ?KafkaConsumer $consumer;

    /**
     * @return mixed|void
     * @throws Exception
     * @throws Throwable
     */
    public function init()
    {
        parent::init();
        array_walk($this->output, function (&$value) {
            $value = false;
        });
        [
            $dsn,
            $topics,
            $options
        ] = ArrayHelper::getValueByArray($this->config, [
            'dsn',
            'topic',
            'options'
        ]);
        if (empty($dsn) || empty($topics)) {
            throw new InvalidConfigException("dsn & topic must be set!");
        }
        $name = md5($dsn);
        /** @var KafkaManager $manager */
        $manager = getDI('kafka');
        $manager->add([
            $name => [
                'type' => 'kafkaconsumer',
                'dsn' => $dsn,
                'set' => $options
            ]
        ]);
        $manager->init();
        $this->consumer = $manager->getKafkaConsumer($name);
        if (!is_array($topics)) {
            $topics = [$topics];
        }
        $this->consumer->subscribe($topics);
    }

    /**
     * @throws Exception
     * @throws Throwable
     */
    public function run()
    {
        /** @var KafkaManager $manager */
        $manager = getDI('kafka');
        $manager->consumeWithKafkaConsumer($this->consumer, function (Message $message) {
            $this->output($message->payload);
        });
    }
}