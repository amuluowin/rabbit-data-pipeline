<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use Rabbit\Base\App;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Nsq\MakeNsqConnection;
use Rabbit\Nsq\Consumer;

class Nsq extends AbstractPlugin
{
    protected array $topics = [];

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
            $nsq = service('nsq')->get($topic['name']);
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
