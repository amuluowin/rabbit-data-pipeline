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
use rabbit\db\redis\RedisLock;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\ExceptionHelper;
use rabbit\redis\Redis;

/**
 * Class Scheduler
 * @package Rabbit\Data\Pipeline
 */
class Scheduler implements SchedulerInterface, InitInterface
{
    /** @var array */
    protected $targets = [];
    /** @var ConfigParserInterface */
    protected $parser;
    /** @var Redis */
    public $redis;
    /** @var int */
    protected $waitTimes = 3;
    /** @var RedisLock */
    public $lock;
    /** @var string */
    protected $name = 'scheduler';
    /** @var array */
    protected $config = [];
    /** @var array */
    protected $taskKeys = [];
    /** @var ISender[] */
    protected $senders;

    /**
     * Scheduler constructor.
     * @param ConfigParserInterface $parser
     */
    public function __construct(ConfigParserInterface $parser)
    {
        $this->parser = $parser;
        $this->config = $this->parser->parse();
        foreach ($this->config as $name => $item) {
            $this->taskKeys[$name] = array_keys($item);
        }
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
        $this->lock = new RedisLock($this->redis);
    }

    /**
     * @param string|null $key
     * @param array $params
     * @throws InvalidArgumentException
     */
    public function run(string $key = null, string $target = null, array $params = []): void
    {
        if ($key === null) {
            foreach (array_keys($this->config) as $key) {
                rgo(function () use ($key, $params) {
                    $this->process((string)$key, $params);
                });
            }
        } elseif (isset($this->config[$key])) {
            if ($target && isset($this->config[$key][$target])) {
                ['taskId' => $taskId, 'input' => $input, 'opt' => $opt, 'request' => $request] = $params;
                $target = $this->getTarget($key, $target);
                $target->setTaskId($taskId);
                $target->setInput($input);
                $target->setOpt($opt);
                $target->setRequest($request);
                rgo(function () use ($target) {
                    $target->process();
                });
            } else {
                rgo(function () use ($key, $params) {
                    $this->process((string)$key, $params);
                });
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
    public function getTarget(string $name, string $key): AbstractPlugin
    {
        if (null !== $target = ArrayHelper::getValue($this->targets, "$name.$key")) {
            return $target;
        }
        $params = $this->config[$name][$key];
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
        $target = ObjectFactory::createObject(
            $class,
            [
                'scName' => $this->name,
                'config' => $params,
                'key' => $key,
                'output' => $output,
                'start' => $start,
                'taskName' => $name,
                'pluginName' => $pluginName,
                'lockEx' => $lockEx,
                'wait' => $wait,
            ],
            false
        );
        if ($target instanceof AbstractSingletonPlugin) {
            $this->targets[$name][$key] = $target;
        }
        return $target;
    }

    /**
     * @param string $task
     * @param array|null $params
     */
    public function process(string $task, array $params = []): void
    {
        /** @var AbstractPlugin $target */
        foreach ($this->config[$task] as $key => $tmp) {
            if (ArrayHelper::getValue($tmp, 'start') === true) {
                $target = $this->getTarget($task, $key);
                $target->setTaskId((string)getDI('idGen')->create());
                $target->setRequest($params);
                $target->process();
            }
        }
    }

    /**
     * @param AbstractPlugin $pre
     * @param string $key
     * @param $data
     * @param bool $transfer
     * @throws Exception
     */
    public function send(AbstractPlugin $pre, string $key, &$data, bool $transfer): void
    {
        try {
            $keyArr = explode(':', $key);
            if (count($keyArr) === 3) {
                [$sender, $address, $target] = $keyArr;
                if (!array_key_exists($sender, $this->senders)) {
                    throw new Exception("Scheduler has no sender name $sender");
                }
                if ($transfer) {
                    rgo(function () use ($sender, $address, $target, $pre, &$data) {
                        $this->senders[$sender]->send($address, $target, $pre, $data);
                    });
                } else {
                    $this->senders[$sender]->send($address, $target, $pre, $data);
                }
            } else {
                /** @var AbstractPlugin $target */
                $target = $this->getTarget($pre->taskName, $key);
                $target->setTaskId($pre->getTaskId());
                $target->setInput($data);
                $opt = $pre->getOpt();
                $target->setOpt($opt);
                $req = $pre->getRequest();
                $target->setRequest($req);
                if ($transfer) {
                    rgo(function () use ($target, $pre, $key) {
                        $target->process();
                        if ($pre->output === []) {
                            App::info("「{$pre->taskName}」 {$pre->getTaskId()} finished!");
                        }
                    });
                } else {
                    $target->process();
                    if ($pre->output === []) {
                        App::info("「{$pre->taskName}」 {$pre->getTaskId()} finished!");
                    }
                }
            }
        } catch (\Throwable $exception) {
            App::error("「{$pre->taskName}」「{$key}」 {$pre->getTaskId()}" . ExceptionHelper::dumpExceptionToString($exception));
            $this->deleteAllLock($pre->getOpt(), $pre->taskName);
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
