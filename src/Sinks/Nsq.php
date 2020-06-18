<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\nsq\Consumer;
use rabbit\nsq\MakeNsqConnection;
use rabbit\nsq\NsqClient;

/**
 * Class Nsq
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Nsq extends AbstractSingletonPlugin
{
    /** @var string */
    protected $topic;
    /** @var string */
    protected $connName;

    /**
     * @param string $class
     * @param string $dsn
     * @param array $pool
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Exception
     */
    protected function createConnection(string $class, string $connName, string $dsn, array $pool): void
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
        MakeNsqConnection::addConnection($class, $connName, $dsn, Consumer::class, $poolConfig);
    }

    /**
     * @return mixed|void
     * @throws Exception
     */
    public function init()
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
            null,
            [
                'pool' => []
            ]
        );
        if ($dsn === null || $class === null || $topic === null) {
            throw new InvalidConfigException("class, dsn,topic must be set in $this->key");
        }
        $this->createConnection($class, $topic, $dsn, $pool);
    }

    /**
     * @throws \Exception
     */
    public function run()
    {
        /** @var NsqClient $nsq */
        $nsq = getDI('nsq')->get($topic);
        if (!is_array($this->input)) {
            $this->input = [$this->input];
        }
        $nsq->publishMulti($this->input);
    }
}