<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Nsq\Consumer;
use Rabbit\Nsq\MakeNsqConnection;
use Rabbit\Nsq\NsqClient;
use ReflectionException;
use Throwable;

/**
 * Class Nsq
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Nsq extends AbstractPlugin
{
    protected ?string $topic;

    /**
     * @param string $class
     * @param string $dsn
     * @param array $pool
     * @throws DependencyException
     * @throws NotFoundException
     * @throws ReflectionException
     * @throws Throwable
     */
    protected function createConnection(string $class, string $dsn, array $pool): void
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
        MakeNsqConnection::addConnection($class, $this->topic, $dsn, Consumer::class, $poolConfig);
    }

    /**
     * @return mixed|void
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws ReflectionException
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $this->topic,
            $class,
            $dsn,
            $pool
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['topic', 'class', 'dsn', 'pool'],
            [
                'pool' => []
            ]
        );
        if ($dsn === null || $class === null || $this->topic === null) {
            throw new InvalidConfigException("class, dsn,topic must be set in $this->key");
        }
        $this->createConnection($class, $dsn, $pool);
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        /** @var NsqClient $nsq */
        $nsq = getDI('nsq')->get($this->topic);
        if (!is_array($msg->data)) {
            $nsq->publish((string)$msg->data);
            return;
        }
        $nsq->publishMulti($msg->data);
    }
}