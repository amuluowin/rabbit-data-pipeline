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
    /** @var bool */
    protected $autoRefresh = false;

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
        if ($this->autoRefresh) {
            $this->refreshConfig();
        }
    }

    protected function refreshConfig(): void
    {
        $fd = inotify_init();
        $watch_descriptor = inotify_add_watch($fd, $this->parser->getPath(), IN_MODIFY);
        swoole_event_add($fd, function ($fd) {
            $events = inotify_read($fd);
            if ($events) {
                foreach ($events as $event) {
                    if (pathinfo($event['name'], PATHINFO_EXTENSION) === 'yaml') {
                        echo App::getServer()->getSwooleServer()->worker_id . " {$event['name']} modify..." . PHP_EOL;
                    }
                }
                if (pathinfo($event['name'], PATHINFO_EXTENSION) === 'yaml') {
                    $this->build();
                }
            }
        });
    }


    /**
     * @param array $params
     * @throws InvalidArgumentException
     * @throws Exception
     */
    public function run(array $params)
    {
        $key = ArrayHelper::remove($params, 'task');
        if ($key === null) {
            foreach (array_keys($this->targets) as $key) {
                if (App::getServer() instanceof CoServer) {
                    $this->process((string)$key);
                } else {
                    getDI(Task::class)->task(['scheduler->process', [$key]]);
                }
            }
        } elseif (isset($this->targets[$key])) {
            if (App::getServer() instanceof CoServer) {
                $this->process((string)$key);
            } else {
                getDI(Task::class)->task(['scheduler->process', [$key]]);
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
        /** @var AbstractPlugin $target */
        foreach ($this->targets[$task] as $target) {
            if ($target->getStart()) {
                $data = [(string)getDI('idGen')->create(), [], $params];
                (clone $target)->process($data);
            }
        }
    }

    /**
     * @param string $taskName
     * @param string $key
     * @param $data
     * @param bool $process
     * @throws Exception
     */
    public function send(string $taskName, string $key, ?string $task_id, &$data, bool $process, array &$opt = []): void
    {
        try {
            /** @var AbstractPlugin $target */
            $target = clone $this->targets[$taskName][$key];
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
                    $params = [$task_id, &$data, &$opt];
                    rgo(function () use (&$target, &$params) {
                        $target->process($params);
                    });
                } else {
                    App::info("Data from $socket->workerId to $workerId", 'Data');
                    $params = ['scheduler->send', [$taskName, $key, $task_id, &$data, false, &$opt]];
                    $socket->send($params, $workerId);
                }
            } elseif ($server instanceof Server) {
                $swooleServer = $server->getSwooleServer();
                $workerId = array_rand(range(0, $swooleServer->setting['worker_num'] +
                isset($swooleServer->setting['task_worker_num']) ? $swooleServer->setting['task_worker_num'] : 0));
                if (!$process || $swooleServer->worker_id === $workerId) {
                    $params = [$task_id, &$data, &$opt];
                    rgo(function () use (&$target, &$params) {
                        $target->process($params);
                    });
                } else {
                    $server->getSwooleServer()->sendMessage([
                        'scheduler->send',
                        [$taskName, $key, $task_id, &$data, false, &$opt]
                    ], $workerId);
                }
            } else {
                $params = [$task_id, &$data, &$opt];
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
