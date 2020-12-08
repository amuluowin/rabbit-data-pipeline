<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\App;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Nsq\MakeNsqConnection;
use Rabbit\Nsq\Consumer;
use ReflectionException;
use Throwable;

/**
 * Class Nsq
 * @package Rabbit\Data\Pipeline\Sources
 */
class Nsq extends AbstractPlugin
{
    /** @var array */
    protected array $topics = [];

    /**
     * @param string $connName
     * @param string $dsn
     * @param string $dsnd
     * @param array $pool
     * @throws DependencyException
     * @throws NotFoundException
     * @throws ReflectionException
     * @throws Throwable
     */
    protected function createConnection(string $connName, string $dsn, string $dsnd, array $pool): void
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
        MakeNsqConnection::addConnection($connName, $dsn, $dsnd, 'consumer', $poolConfig);
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
        $this->topics = $this->config['topics'] ?? [];
        foreach ($this->topics as $topic => $config) {
            [
                $dsn,
                $dsnd,
                $pool
            ] = ArrayHelper::getValueByArray(
                $config,
                ['dsn', 'dsnd', 'pool'],
                [
                    'pool' => []
                ]
            );
            if ($dsn === null || $dsnd === null) {
                throw new InvalidConfigException("dsn & dsnd must be set in $this->key");
            }
            $this->topics[$topic]['isRunning'] = false;
            $this->topics[$topic]['name'] = md5($dsn);
            $this->createConnection($this->topics[$topic]['name'], $dsn, $dsnd, $pool);
        }
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        if (empty($topic = ArrayHelper::getValue($msg->data, 'topic'))) {
            $needRun = $this->topics;
        } else {
            $needRun = [$this->topics[$topic]];
        }
        foreach ($needRun as $topic => $config) {
            if ($this->topics[$topic]['isRunning']) {
                App::warning("$topic is running..");
                return;
            }
            $this->topics[$topic]['isRunning'] = true;
            /** @var Consumer $nsq */
            $nsq = getDI('nsq')->get($topic['name']);
            [$name, $channel] = explode(':', $topic);
            $nsq->subscribe($name, $channel, [
                'rdy' => $config['rdy'] ?? swoole_cpu_num(),
                'timeout' => $config['timeout'] ?? 5
            ], function (array $message) use ($msg): void {
                $out = clone $msg;
                $out->data = $message;
                $this->sink($out);
            });
        }
    }
}
