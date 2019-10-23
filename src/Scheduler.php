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
use rabbit\httpserver\CoServer;
use rabbit\server\Server;
use rabbit\server\Task\Task;

class Scheduler implements InitInterface
{
    /** @var array */
    protected $targets = [];
    /** @var ConfigParserInterface */
    protected $parser;

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
        foreach ($this->parser->parse() as $task => $config) {
            $this->build($task, $config);
        }
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
     * @param string $name
     * @param array $config
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     */
    protected function build(string $name, array $config): void
    {
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
            $this->targets[$name][$key] = ObjectFactory::createObject(
                $class,
                [
                    'config' => $params,
                    'key' => $key,
                    'output' => $output,
                    'start' => $start,
                    'taskName' => $name,
                    'init()' => [],
                ],
                false
            );
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
                $target->input($params);
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
    public function send(string $taskName, string $key, &$data, bool $process): void
    {
        if (empty($data)) {
            App::warning("$taskName $key input empty data,ignore!");
            return;
        }
        /** @var CoServer $server */
        $server = App::getServer();
        if ($server instanceof CoServer) {
            $socket = $server->getProcessSocket();
            $workerId = array_rand($socket->getWorkerIds());
            if (!$process || $socket->workerId === $workerId) {
                $this->targets[$taskName][$key]->input($data);
            } else {
                App::info("Data from $socket->workerId to $workerId", 'Data');
                $params = ['scheduler->send', [$taskName, $key, &$data, false]];
                $socket->send($params, $workerId);
            }
        } elseif ($server instanceof Server) {
            $swooleServer = $server->getSwooleServer();
            $workerId = array_rand(range(0, $swooleServer->setting['worker_num'] +
            isset($swooleServer->setting['task_worker_num']) ? $swooleServer->setting['task_worker_num'] : 0));
            if (!$process || $swooleServer->worker_id === $workerId) {
                $this->targets[$taskName][$key]->input($data);
            } else {
                $server->getSwooleServer()->sendMessage([
                    'scheduler->send',
                    [$taskName, $key, &$data, false]
                ], $workerId);
            }
        } else {
            $this->targets[$taskName][$key]->input($data);
        }
    }
}
