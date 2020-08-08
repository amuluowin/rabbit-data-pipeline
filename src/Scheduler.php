<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use DI\DependencyException;
use DI\NotFoundException;
use Exception;
use Rabbit\Base\App;
use Rabbit\Base\Contract\InitInterface;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Base\Helper\LockHelper;
use Rabbit\DB\Redis\RedisLock;
use ReflectionException;
use Throwable;

/**
 * Class Scheduler
 * @package Rabbit\Data\Pipeline
 */
class Scheduler implements SchedulerInterface, InitInterface
{
    /** @var array */
    protected array $targets = [];
    /** @var ConfigParserInterface */
    protected ConfigParserInterface $parser;
    /** @var string */
    protected string $name = 'scheduler';
    /** @var array */
    protected array $config = [];
    /** @var ISender[] */
    protected array $senders = [];
    /** @var string */
    protected string $redisKey = 'default';

    /**
     * Scheduler constructor.
     * @param ConfigParserInterface $parser
     */
    public function __construct(ConfigParserInterface $parser)
    {
        $this->parser = $parser;
        $this->config = $this->parser->parse();
    }

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
    {
        LockHelper::add('redis', new RedisLock(getDI('redis')->get($this->redisKey)));
    }

    /**
     * @param string|null $key
     * @param string|null $target
     * @param array $params
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws Throwable
     */
    public function run(string $key = null, string $target = null, array $params = []): array
    {
        $taskResult = [];
        if ($key === null) {
            foreach (array_keys($this->config) as $key) {
                $taskResult[$key] = $this->start((string)$key, $params);
            }
        } elseif (isset($this->config[$key])) {
            if ($target && isset($this->config[$key][$target])) {
                $runTarget = $this->getTarget($key, $target);
                $msg = create(Message::class, array_merge(['redis' => getDI('redis')->get($this->redisKey)], $params), false);
                rgo(fn() => $runTarget->process($msg));
                $taskResult = ["$key.$target" => 'proxy run success'];
            } else {
                $taskResult[$key] = $this->start((string)$key, $params);
            }
        } else {
            throw new InvalidArgumentException("No such name $key");
        }
        return $taskResult;
    }

    private function start(string $key, array &$params): string
    {
        $result = '';
        $lock = ArrayHelper::getValue($this->config[$key], 'lock');
        if ($lock && false === lock('redis', function () use ($key, &$params) {
                rgo(fn() => $this->process($key, $params));
            }, $this->name . '.' . $key, $lock)) {
            App::warning("$key is running");
            $result = "$key is running";
        } else {
            $result = "$key start run";
            rgo(fn() => $this->process($key, $params));
        }
        return $result;
    }

    /**
     * @param string $name
     * @param string $key
     * @return AbstractPlugin
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws ReflectionException
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
        $errHandler = ArrayHelper::remove($params, 'errHandler');
        if (is_string($output)) {
            $output = [$output => true];
        }
        $target = create(
            $class,
            [
                'scName' => $this->name,
                'config' => $params,
                'key' => $key,
                'output' => $output,
                'start' => $start,
                'taskName' => $name,
                'wait' => $wait,
                'errHandler' => $errHandler
            ],
            false
        );
        $this->targets[$name][$key] = $target;
        return $target;
    }

    /**
     * @param string $task
     * @param array|null $params
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws ReflectionException
     * @throws Throwable
     */
    public function process(string $task, array $params = []): void
    {
        /** @var AbstractPlugin $target */
        foreach ($this->config[$task] as $key => $tmp) {
            if ($key === 'lock') {
                continue;
            }
            if (ArrayHelper::getValue($tmp, 'start') === true) {
                $target = $this->getTarget($task, $key);
                $msg = create(Message::class, ['redisKey' => $this->redisKey, 'taskName' => $task, 'taskId' => (string)getDI('idGen')->create()], false);
                $target->process($msg);
            }
        }
    }

    /**
     * @param Message $msg
     * @param string $key
     * @throws Throwable
     */
    public function next(Message $msg, string $key): void
    {
        try {
            $keyArr = explode(':', $key);
            if (count($keyArr) === 3) {
                [$sender, $address, $target] = $keyArr;
                if (!array_key_exists($sender, $this->senders)) {
                    throw new Exception("Scheduler has no sender name $sender");
                }
                $this->senders[$sender]->send($address, $target, $msg);
            } else {
                $target = $this->getTarget($msg->taskName, $key);
                $target->process($msg);
                if ($target->output === []) {
                    App::info("「{$msg->taskName}」 {$msg->taskId} finished!");
                }
            }
        } catch (Throwable $exception) {
            App::error("「{$msg->taskName}」「{$key}」 {$msg->taskId}" . ExceptionHelper::dumpExceptionToString($exception));
            $msg->deleteAllLock();
        }
    }
}
