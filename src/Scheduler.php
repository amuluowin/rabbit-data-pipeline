<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use common\Exception\InvalidArgumentException;
use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use rabbit\App;
use rabbit\contract\InitInterface;
use rabbit\core\ObjectFactory;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\ExceptionHelper;
use rabbit\httpserver\CoServer;
use rabbit\redis\Redis;
use rabbit\server\Server;
use rabbit\server\Task\Task;

class Scheduler implements InitInterface
{
    /** @var array */
    protected $targets = [];
    /** @var ConfigParserInterface */
    protected $parser;
    /** @var Redis */
    protected $redis;

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
        $this->redis = getDI('redis');
    }


    /**
     * @param array $params
     * @throws InvalidArgumentException
     * @throws Exception
     */
    public function run(array $params)
    {
        $key = ArrayHelper::remove($params, 'task');
        if (App::getServer() instanceof CoServer) {
            $this->process((string)$key);
        } else {
            getDI(Task::class)->task(['scheduler->process', [$key]]);
        }
    }

    /**
     * @param string $name
     * @param array $config
     * @return array
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     */
    protected function build(string $name, array $config): array
    {
        $targets = [];
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
            $targets[$name][$key] = ObjectFactory::createObject(
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
        return $targets;
    }

    /**
     * @param string $task
     * @param array|null $params
     */
    public function process(string $task = null, array $params = []): void
    {
        foreach ($this->parser->parse() as $task => $config) {
            $tasks = $this->build($task, $config);
        }
        if ($key === null) {
            /** @var AbstractPlugin $target */
            foreach ($tasks as $targets) {
                foreach ($targets as $target) {
                    if ($target->getStart()) {
                        $data = [(string)getDI('idGen')->create(), $params];
                        $target->process($data);
                    }
                }
            }
        } elseif (isset($tasks[$key])) {
            if (App::getServer() instanceof CoServer) {
                /** @var AbstractPlugin $target */
                foreach ($tasks[$key] as $target) {
                    if ($target->getStart()) {
                        $data = [(string)getDI('idGen')->create(), $params];
                        $target->process($data);
                    }
                }
            } else {
                getDI(Task::class)->task(['scheduler->process', [$key]]);
            }
        } else {
            throw new InvalidArgumentException("No such target $key");
        }
    }

    /**
     * @param string $taskName
     * @param string $key
     * @param $data
     * @param bool $process
     * @throws Exception
     */
    public function send(string $taskName, string $key, ?string $task_id, &$data, bool $process): void
    {
        try {
            foreach ($this->parser->parse() as $task => $config) {
                $tasks = $this->build($task, $config);
            }
            /** @var AbstractPlugin $target */
            $target = $tasks[$taskName][$key];
            if (empty($data)) {
                App::warning("$taskName $key input empty data,ignore!");
                $this->redis->del($task_id);
                return;
            }

            /** @var CoServer $server */
            $server = App::getServer();
            if ($server instanceof CoServer) {
                $socket = $server->getProcessSocket();
                $workerId = array_rand($socket->getWorkerIds());
                if (!$process || $socket->workerId === $workerId) {
                    $params = [$task_id, &$data];
                    rgo(function () use (&$target, &$params) {
                        $target->process($params);
                    });
                } else {
                    App::info("Data from $socket->workerId to $workerId", 'Data');
                    $params = ['scheduler->send', [$taskName, $key, $task_id, &$data, false]];
                    $socket->send($params, $workerId);
                }
            } elseif ($server instanceof Server) {
                $swooleServer = $server->getSwooleServer();
                $workerId = array_rand(range(0, $swooleServer->setting['worker_num'] +
                isset($swooleServer->setting['task_worker_num']) ? $swooleServer->setting['task_worker_num'] : 0));
                if (!$process || $swooleServer->worker_id === $workerId) {
                    $params = [$task_id, &$data];
                    rgo(function () use (&$target, &$params) {
                        $target->process($params);
                    });
                } else {
                    $server->getSwooleServer()->sendMessage([
                        'scheduler->send',
                        [$taskName, $key, $task_id, &$data, false]
                    ], $workerId);
                }
            } else {
                $params = [$task_id, &$data];
                rgo(function () use (&$target, &$params) {
                    $target->process($params);
                });
            }
        } catch (\Throwable $exception) {
            App::error(ExceptionHelper::dumpExceptionToString($exception));
            $this->redis->del($task_id);
        }
    }
}
