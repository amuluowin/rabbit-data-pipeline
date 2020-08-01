<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Rdkafka\KafkaManager;
use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Throwable;

/**
 * Class RdKafka
 * @package Rabbit\Data\Pipeline\Sources
 */
class RdKafka extends AbstractPlugin
{
    /** @var KafkaConsumer */
    protected ?KafkaConsumer $consumer;

    /**
     * @return mixed|void
     * @throws Exception
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
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
     * @param \Rabbit\Data\Pipeline\Message $msg
     * @throws Exception
     * @throws Throwable
     */
    public function run(\Rabbit\Data\Pipeline\Message $msg): void
    {
        /** @var KafkaManager $manager */
        $manager = getDI('kafka');
        $manager->consumeWithKafkaConsumer($this->consumer, function (Message $message) use ($msg): void {
            $tmp = clone $msg;
            $tmp->data = $message->payload;
            $this->sink($tmp);
        });
    }
}