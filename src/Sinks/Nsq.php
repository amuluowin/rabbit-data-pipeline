<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Nsq\Producer;
use Rabbit\Nsq\MakeNsqConnection;

class Nsq extends AbstractPlugin
{
    protected ?string $topic;

    protected string $name;

    protected function createConnection(string $dsn, string $dsnd, array $pool): void
    {
        [
            $poolConfig['min'],
            $poolConfig['max'],
            $poolConfig['wait'],
            $poolConfig['retry']
        ] = ArrayHelper::getValueByArray(
            $pool,
            ['min', 'max', 'wait', 'retry'],
            [1, 1, 0, 3]
        );
        $this->name = md5($dsn);
        MakeNsqConnection::addConnection($this->name, $dsn, $dsnd, 'producer', $poolConfig);
    }

    public function init(): void
    {
        parent::init();
        [
            $this->topic,
            $dsn,
            $dsnd,
            $pool
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['topic', 'dsn', 'dsnd', 'pool'],
            [
                'pool' => []
            ]
        );
        if ($dsn === null || $dsnd === null || $this->topic === null) {
            throw new InvalidConfigException("dsn,topic must be set in $this->key");
        }
        $this->createConnection($dsn, $dsnd, $pool);
        /** @var Producer $nsq */
        $nsq = service('nsq')->getProducer($this->name);
        $nsq->makeTopic($this->topic);
    }

    public function run(Message $msg): void
    {
        /** @var Producer $nsq */
        $nsq = service('nsq')->getProducer($this->name);
        if (!is_array($msg->data)) {
            $nsq->publish($this->topic, (string)$msg->data);
            return;
        }
        $nsq->publishMulti($this->topic, $msg->data);
    }
}
