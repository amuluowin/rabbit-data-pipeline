<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Co\System;
use common\Exception\InvalidArgumentException;
use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use rabbit\App;
use rabbit\contract\InitInterface;
use rabbit\core\ObjectFactory;
use rabbit\exception\InvalidConfigException;
use rabbit\exception\NotSupportedException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\ExceptionHelper;
use rabbit\httpserver\CoServer;
use rabbit\redis\Redis;
use Swoole\Table;

class Scheduler implements SchedulerInterface, InitInterface
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
    /** @var Table */
    protected $taskTable;
    /** @var int */
    protected $waitTimes = 3;

    /**
     * Scheduler constructor.
     * @param ConfigParserInterface $parser
     */
    public function __construct(ConfigParserInterface $parser)
    {
        $this->parser = $parser;
        $this->taskTable = new Table(1024);
        $this->taskTable->column('taskName', Table::TYPE_STRING, 64);
        $this->taskTable->column('key', Table::TYPE_STRING, 64);
        $this->taskTable->column('request', Table::TYPE_STRING, 1024);
        $this->taskTable->column('stop', Table::TYPE_INT, 1);
        $this->taskTable->create();
    }

    /**
     * @return mixed|void
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     */
    public function init()
    {
        $this->build($this->parser->parse());
        $this->redis = getDI('redis');
        if ($this->autoRefresh) {
            $this->refreshConfig();
        }
    }

    /**
     * @param string $taskName
     * @param string $name
     * @return AbstractPlugin|null
     * @throws Exception
     */
    public function getTarget(string $taskName, string $name): ?AbstractPlugin
    {
        $waitTime = 0;
        /** @var AbstractPlugin $target */
        while ((empty($this->targets) || !isset($this->targets[$taskName]) || !isset($this->targets[$taskName][$name])) && (++$waitTime <= $this->waitTimes)) {
            App::warning("The $taskName is building wait {$this->waitTimes}s");
            System::sleep($waitTime * 3);
        }
        return $this->targets[$taskName][$name];
    }


    /**
     * @return array
     */
    public function getTasks(): array
    {
        $table = [];
        foreach ($this->taskTable as $key => $item) {
            $item['request'] = \msgpack_unpack($item['request']);
            $table[$key] = $item;
        }
        return $table;
    }

    /**
     * @param AbstractPlugin $target
     */
    protected function setTask(AbstractPlugin $target): void
    {
        $this->taskTable->set($target->task_id, [
            'taskName' => $target->taskName,
            'key' => $target->key,
            'request' => \msgpack_pack($target->request),
            'stop' => 0
        ]);
    }

    /**
     * @param string $task_id
     */
    public function stopTask(string $task_id): void
    {
        $this->taskTable->set($task_id, ['stop' => 1]);
    }

    /**
     * @param string|null $key
     * @param array $params
     * @throws InvalidArgumentException
     */
    public function run(string $key = null, array $params = []): void
    {
        $server = App::getServer();
        if ($key === null) {
            foreach (array_keys($this->targets) as $key) {
                if ($server === null || $server instanceof CoServer) {
                    $this->process((string)$key, $params);
                } else {
                    throw new NotSupportedException("Do not support Swoole\Server");
                }
            }
        } elseif (isset($this->targets[$key])) {
            if ($server === null || $server instanceof CoServer) {
                $this->process((string)$key, $params);
            } else {
                throw new NotSupportedException("Do not support Swoole\Server");
            }
        } else {
            throw new InvalidArgumentException("No such target $key");
        }
    }

    /**
     * @param array $configs
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     */
    public function build(array $configs): void
    {
        foreach ($configs as $name => $config) {
            foreach ($config as $key => $params) {
                $class = ArrayHelper::remove($params, 'type');
                if (!$class) {
                    throw new InvalidConfigException("The type must be set in $key");
                }
                $output = ArrayHelper::remove($params, 'output', []);
                $start = ArrayHelper::remove($params, 'start', false);
                $wait = ArrayHelper::remove($params, 'wait', false);
                if (is_string($output)) {
                    $output = [$output => true];
                }
                $lockEx = ArrayHelper::remove($params, 'lockEx', 30);
                $pluginName = ArrayHelper::remove($params, 'name', uniqid());
                $this->targets[$name][$key] = ObjectFactory::createObject(
                    $class,
                    [
                        'scheduler' => $this,
                        'config' => $params,
                        'key' => $key,
                        'output' => $output,
                        'start' => $start,
                        'taskName' => $name,
                        'pluginName' => $pluginName,
                        'lockEx' => $lockEx,
                        'wait' => $wait,
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
                $current->request = $params;
                $this->setTask($current);
                $current->process();
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
    public function send(string $taskName, string $key, ?string $task_id, &$data, ?int $transfer, array $opt = [], array $request = [], bool $wait = false): void
    {
        try {
            if ($this->taskTable->get($task_id, 'stop') === 1) {
                $this->taskTable->del($task_id);
                App::warning("「$taskName」 $task_id stoped by user!");
                return;
            }
            if ($transfer === null) {
                $target = clone $this->getTarget($taskName, $key);
                $target->task_id = $task_id;
                $target->input =& $data;
                $target->opt = $opt;
                $target->request =& $request;
                $this->setTask($target);
                $target->process();
            } else {
                $this->transSend($taskName, $key, $task_id, $data, $transfer, $opt, $request, $wait);
            }
            if (end($this->targets[$taskName])->key === $key) {
                App::info("「{$taskName}」 finished!");
            }
        } catch (\Throwable $exception) {
            App::error("「{$taskName}」「{$key}」" . ExceptionHelper::dumpExceptionToString($exception));
            $this->deleteAllLock($opt, $taskName);
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
    protected function transSend(string $taskName, string $key, ?string $task_id, &$data, ?int $transfer, array &$opt = [], array &$request = [], bool $wait = false): void
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
                $workerId = $transfer % count($ids);
                $workerId === $socket->workerId && $workerId++;
            } else {
                unset($ids[$socket->workerId]);
                $workerId = array_rand($ids);
            }
            App::info("Data from worker $socket->workerId to $workerId", 'Data');
            $params = ["{$this->name}->send", [$taskName, $key, $task_id, &$data, null, &$opt, &$request, $wait]];
            $socket->send($params, $workerId);
        } else {
            throw new NotSupportedException("Do not support Swoole\Server");
        }
    }

    /**
     * @return int
     */
    public function getLock(string $key = null, $lockEx = 60): bool
    {
        return (bool)$this->redis->set($key, true, ['NX', 'EX' => $lockEx]);
    }

    /**
     * @param array $opt
     */
    public function deleteAllLock(array $opt = [], string $taskName = ''): void
    {
        $locks = isset($opt['Locks']) ? $opt['Locks'] : [];
        foreach ($locks as $lock) {
            !is_string($lock) && $lock = strval($lock);
            $this->deleteLock($lock, $taskName);
        }
    }

    /**
     * @param string|null $key
     * @param string $taskName
     * @return int
     * @throws Exception
     */
    public function deleteLock(string $key = null, string $taskName = ''): int
    {
        if ($flag = $this->redis->del($key)) {
            App::warning("「{$taskName}」 Delete Lock: " . $key);
        }
        return (int)$flag;

    }
}
