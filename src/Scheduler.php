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
    private $autoRefresh = false;
    /** @var string */
    protected $name = 'scheduler';

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
                    $this->process((string)$key, $params);
                } else {
                    getDI(Task::class)->task(["{$this->name}->process", [$key, $params]]);
                }
            }
        } elseif (isset($this->targets[$key])) {
            if ($server === null || $server instanceof CoServer) {
                $this->process((string)$key, $params);
            } else {
                getDI(Task::class)->task(["{$this->name}->process", [$key, $params]]);
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
                $current = clone $target;
                $current->task_id = (string)getDI('idGen')->create();
                $current->input = $params;
                $current->run();
            }
        }
    }

    /**
     * @param string $taskName
     * @param string $key
     * @param string|null $task_id
     * @param $data
     * @param int|null $transfer
     * @param array $opt
     * @throws Exception
     */
    public function send(string $taskName, string $key, ?string $task_id, &$data, ?int $transfer, array $opt = []): void
    {
        try {
            /** @var AbstractPlugin $target */
            $target = clone $this->targets[$taskName][$key];
            if (empty($data)) {
                App::warning("$taskName $key input empty data,ignore!");
                $this->redis->del($task_id);
                return;
            }

            $target->task_id = $task_id;
            $target->input =& $data;
            $target->opt =& $opt;

            /** @var CoServer $server */
            if ($transfer === null) {
                rgo(function () use ($target) {
                    $target->run();
                });
            } else {
                $this->transSend($taskName, $key, $task_id, $data, $transfer, $opt);
            }
        } catch (\Throwable $exception) {
            App::error(ExceptionHelper::dumpExceptionToString($exception));
            $this->redis->del($task_id);
        }
    }

    /**
     * @param string $taskName
     * @param string $key
     * @param string|null $task_id
     * @param $data
     * @param int|null $transfer
     * @param array $opt
     * @throws Exception
     */
    protected function transSend(string $taskName, string $key, ?string $task_id, &$data, ?int $transfer, array $opt = []): void
    {
        $server = App::getServer();
        if ($server === null || $server instanceof CoServer) {
            if ($server === null) {
                $socket = getDI('socketHandle');
            } else {
                $socket = $server->getProcessSocket();
            }
            $ids = $socket->getWorkerIds();
            if ($transfer > -1) {
                $transfer++;
                $workerId = $transfer % count($ids);
            } else {
                unset($ids[$socket->workerId]);
                $workerId = array_rand($ids);
            }
            App::info("Data from $socket->workerId to $workerId", 'Data');
            $params = ["{$this->name}->send", [$taskName, $key, $task_id, &$data, null, &$opt]];
            $socket->send($params, $workerId);
        } else {
            $swooleServer = $server->getSwooleServer();
            $ids = range(0, $swooleServer->setting['worker_num'] +
            isset($swooleServer->setting['task_worker_num']) ? $swooleServer->setting['task_worker_num'] : 0);
            if ($transfer > -1) {
                $transfer++;
                $workerId = $transfer % count($ids);
            } else {
                unset($ids[$swooleServer->worker_id]);
                $workerId = array_rand($ids);
            }
            $server->getSwooleServer()->sendMessage([
                "{$this->name}->send",
                [$taskName, $key, $task_id, &$data, null, &$opt]
            ], $workerId);
        }
    }
}
