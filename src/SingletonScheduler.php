<?php


namespace Rabbit\Data\Pipeline;

use common\Exception\InvalidArgumentException;
use DI\DependencyException;
use DI\NotFoundException;
use rabbit\App;
use rabbit\contract\InitInterface;
use rabbit\core\ObjectFactory;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\ExceptionHelper;
use rabbit\httpserver\CoServer;
use rabbit\redis\Redis;
use rabbit\server\Task\Task;

/**
 * Class SingletonScheduler
 * @package Rabbit\Data\Pipeline
 */
class SingletonScheduler implements InitInterface
{
    /** @var array */
    protected $targets = [];
    /** @var ConfigParserInterface */
    protected $parser;
    /** @var Redis */
    protected $redis;
    /** @var string */
    protected $name = 'singletonscheduler';

    /**
     * Scheduler constructor.
     * @param ConfigParserInterface $parser
     */
    public function __construct(ConfigParserInterface $parser)
    {
        $this->parser = $parser;
    }

    /**
     * @return mixed|void
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     */
    public function init()
    {
        $this->build();
        $this->redis = getDI('redis');
    }


    /**
     * @param string|null $key
     * @param array $params
     * @throws InvalidArgumentException
     */
    public function run(string $key = null, array $params = [])
    {
        $server = App::getServer();
        if ($key === null) {
            foreach (array_keys($this->targets) as $key) {
                if ($server === null || $server instanceof CoServer) {
                    $this->process((string)$key);
                } else {
                    getDI(Task::class)->task(["{$this->name}->process", [$key]]);
                }
            }
        } elseif (isset($this->targets[$key])) {
            if ($server === null || $server instanceof CoServer) {
                $this->process((string)$key);
            } else {
                getDI(Task::class)->task(["{$this->name}->process", [$key]]);
            }
        } else {
            throw new InvalidArgumentException("No such target $key");
        }
    }

    /**
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     */
    protected function build(): void
    {
        foreach ($this->parser->parse() as $name => $config) {
            foreach ($config as $key => $params) {
                $class = ArrayHelper::remove($params, 'type');
                if (!$class) {
                    throw new InvalidConfigException("The type must be set in $key");
                }
                $output = ArrayHelper::remove($params, 'output', []);
                $start = ArrayHelper::remove($params, 'start', false);
                if (is_string($output)) {
                    $output = [$output => false];
                }
                $lockEx = ArrayHelper::remove($params, 'lockEx', 30);
                $this->targets[$name][$key] = ObjectFactory::createObject(
                    $class,
                    [
                        'config' => $params,
                        'key' => $key,
                        'output' => $output,
                        'start' => $start,
                        'taskName' => $name,
                        'lockEx' => $lockEx,
                        'init()' => [],
                    ],
                    false
                );
            }
        }
    }

    /**
     * @param string $task
     * @param array|null $params
     */
    public function process(string $task, array $params = []): void
    {
        /** @var AbstractSingletonPlugin $target */
        foreach ($this->targets[$task] as $target) {
            if ($target->getStart()) {
                $target->task_id = (string)getDI('idGen')->create();
                $opt = [];
                $target->process($params, $opt);
            }
        }
    }

    /**
     * @param string $taskName
     * @param string $key
     * @param string|null $task_id
     * @param $data
     * @param bool $transfer
     * @param array $opt
     * @throws Exception
     */
    public function send(string $taskName, string $key, ?string $task_id, &$data, ?int $transfer, array $opt = []): void
    {
        try {
            /** @var AbstractSingletonPlugin $target */
            $target = $this->targets[$taskName][$key];
            if (empty($data)) {
                App::warning("$taskName $key input empty data,ignore!");
                $this->redis->del($task_id);
                return;
            }

            if ($transfer === null) {
                rgo(function () use ($target, &$data, &$opt) {
                    $target->process($data, $opt);
                });
            } else {
                /** @var CoServer $server */
                $server = App::getServer();
                if ($server === null || $server instanceof CoServer) {
                    if ($server === null) {
                        $socket = getDI('socketHandle');
                    } else {
                        $socket = $server->getProcessSocket();
                    }
                    $ids = $socket->getWorkerIds();
                    $socket->workerId === $transfer && $transfer++;
                    if ($transfer > -1) {
                        $workerId = $transfer % count($ids);
                    } else {
                        $workerId = array_rand($ids);
                    }
                    App::info("Data from $socket->workerId to $workerId", 'Data');
                    $params = ["{$this->name}->send", [$taskName, $key, $task_id, &$data, null, &$opt]];
                    $socket->send($params, $workerId);
                } else {
                    $swooleServer = $server->getSwooleServer();
                    $ids = range(0, $swooleServer->setting['worker_num'] +
                    isset($swooleServer->setting['task_worker_num']) ? $swooleServer->setting['task_worker_num'] : 0);
                    $swooleServer->worker_id === $transfer && $transfer++;
                    if ($transfer > -1) {
                        $workerId = $transfer % count($ids);
                    } else {
                        $workerId = array_rand($ids);
                    }
                    $server->getSwooleServer()->sendMessage([
                        "{$this->name}->send",
                        [$taskName, $key, $task_id, &$data, null, &$opt]
                    ], $workerId);
                }
            }
        } catch (\Throwable $exception) {
            App::error(ExceptionHelper::dumpExceptionToString($exception));
            $this->redis->del($task_id);
        }
    }
}
