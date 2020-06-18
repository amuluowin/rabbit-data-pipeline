<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use rabbit\App;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\nsq\Consumer;
use rabbit\nsq\MakeNsqConnection;
use rabbit\nsq\message\Message;
use rabbit\nsq\NsqClient;

/**
 * Class Nsq
 * @package Rabbit\Data\Pipeline\Sources
 */
class Nsq extends AbstractSingletonPlugin
{
    /** @var array */
    protected $topics = [];
    /** @var string */
    protected $connName;

    /**
     * @param string $connName
     * @param string $dsn
     * @param string $dsnd
     * @param array $pool
     * @throws \DI\DependencyException
     * @throws \DI\NotFoundException
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
            null,
            [1, 1, 0, 3]
        );
        MakeNsqConnection::addConnection($connName, $dsn, $dsnd, Consumer::class, $poolConfig);
    }

    /**
     * @return mixed|void
     * @throws Exception
     */
    public function init()
    {
        parent::init();
        $this->topics = ArrayHelper::getValue($this->config, 'topics', []);
        foreach ($this->topics as $topic => $config) {
            [
                $dsn,
                $dsnd,
                $pool
            ] = ArrayHelper::getValueByArray(
                $config,
                ['dsn', 'dsnd', 'pool'],
                null,
                [
                    'pool' => []
                ]
            );
            if ($dsn === null || $dsnd === null) {
                throw new InvalidConfigException("dsn & dsnd must be set in $this->key");
            }
            $this->topics[$topic]['isRunning'] = false;
            $this->createConnection($topic, $dsn, $dsnd, $pool);
        }
    }

    /**
     * @throws \Exception
     */
    public function run()
    {
        if (empty($topic = ArrayHelper::getValue($this->input, 'topic'))) {
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
            /** @var NsqClient $nsq */
            $nsq = getDI('nsq')->get($topic);
            $nsq->subscribe([
                'rdy' => ArrayHelper::getValue($config, 'rdy', swoole_cpu_num()),
                'timeout' => ArrayHelper::getValue($config, 'timeout', 5)
            ], function (array $message) {
                $this->output($message);
            });
        }
    }
}