<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Exception;
use Throwable;
use Rabbit\Base\App;
use Rabbit\Cron\CronJob;
use ReflectionException;
use DI\NotFoundException;
use DI\DependencyException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\InvalidArgumentException;

/**
 * Class Scheduler
 * @package Rabbit\Data\Pipeline
 */
class Scheduler implements SchedulerInterface
{
    protected array $targets = [];
    protected ConfigParserInterface $parser;
    protected string $name = 'scheduler';
    protected array $config = [];
    protected array $senders = [];
    protected string $redisKey = 'default';
    protected ?CronJob $cron = null;

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
     * @author Albert <63851587@qq.com>
     * @return array
     */
    public function getConfig(): array
    {
        return $this->config;
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
    public function run(string $key, string $target = null, array $params = []): array
    {
        $taskResult = [];
        if (isset($this->config[$key])) {
            try {
                if ($target && isset($this->config[$key][$target])) {
                    $runTarget = $this->getTarget($key, $target);
                    $msg = create(Message::class, array_merge(['redis' => getDI('redis')->get($this->redisKey)], $params), false);
                    $runTarget->process($msg);
                    $taskResult[$key] = [$target => 'proxy run success'];
                } else {
                    if (false === ArrayHelper::getValue($this->config[$key], "singleton", true)) {
                        $this->config[$key] = $this->parser->parseTask($key);
                    }
                    $taskResult[$key] = $this->start((string)$key, $params);
                }
            } catch (Throwable $exception) {
                App::error(ExceptionHelper::dumpExceptionToString($exception));
                $taskResult[$key] = "faild!msg=" . $exception->getMessage();
            }
        } else {
            throw new InvalidArgumentException("No such name $key");
        }
        return $taskResult;
    }

    /**
     * @Author Albert 63851587@qq.com
     * @DateTime 2020-11-03
     * @param array $tasks
     * @param integer $wait
     * @param array $params
     * @return array
     */
    public function multi(array $tasks, int $wait = -1, array $params = []): array
    {
        $taskResult = [];
        wgeach($tasks, function (int $i, string $key) use (&$taskResult, $params) {
            $taskResult = array_merge($taskResult, $this->run($key, null, $params));
        }, $wait);
        return $taskResult;
    }

    /**
     * @author Albert <63851587@qq.com>
     * @param string $key
     * @param array $params
     * @return string
     */
    private function start(string $key, array &$params): string
    {
        $result = '';
        $lock = ArrayHelper::getValue($this->config[$key], 'lock');
        $expression = (string)ArrayHelper::getValue($this->config[$key], 'cron');
        $func = function (string $key, string $expression, array &$params) {
            if ($this->cron && $expression !== '') {
                if ((int)$expression >= 0) {
                    while (true) {
                        $this->process($key, $params);
                        App::info("$key finished once! Go on with {$expression}s later");
                        (int)$expression && sleep((int)$expression);
                    }
                } else {
                    $this->cron->add($key, [$expression, function () use ($key, &$params) {
                        $this->process($key, $params);
                    }]);
                    $this->cron->run($key);
                    App::info("$key run with cron: $expression");
                }
            } else {
                $this->process($key, $params);
            }
        };
        if ($lock && false === rlock(function () use ($func, $key, $expression, &$params) {
            $func($key, $expression, $params);
        }, false, $this->name . '.' . $key, $lock)) {
            App::warning("$key is running");
            $result = "$key is running";
        } else {
            $result = "$key start run";
            $func($key, $expression, $params);
        }

        return $result;
    }

    /**
     * @param string $name
     * @param string $key
     * @param bool $singleton
     * @return AbstractPlugin
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws ReflectionException
     */
    public function getTarget(string $name, string $key, bool $singleton = true): AbstractPlugin
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
        $canEmpty = ArrayHelper::remove($params, 'canEmpty', false);
        $errHandler = ArrayHelper::remove($params, 'errHandler');
        $alarm = ArrayHelper::remove($params, 'alarm');
        $singleton = ArrayHelper::getValue($this->config[$name], 'singleton', true);
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
                'canEmpty' => $canEmpty,
                'wait' => $wait,
                'errHandler' => $errHandler,
                'alarm' => $alarm
            ],
            false
        );
        if ($singleton) {
            $this->targets[$name][$key] = $target;
        }
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
                $msg = create(Message::class, ['redisKey' => $this->redisKey, 'taskName' => $task, 'taskId' => (string)getDI('idGen')->nextId()], false);
                $target->process($msg);
            }
        }
    }

    /**
     * @param Message $msg
     * @param string $key
     * @throws Throwable
     */
    public function next(Message $msg, string $key, float $wait = 0): void
    {
        try {
            $keyArr = explode(':', $key);
            if (count($keyArr) === 3) {
                [$sender, $address, $target] = $keyArr;
                if (!array_key_exists($sender, $this->senders)) {
                    throw new Exception("Scheduler has no sender name $sender");
                }
                $this->senders[$sender]->send($target, $msg, (string)$address, $wait);
            } else {
                $target = $this->getTarget($msg->taskName, $key);
                $target->process($msg);
            }
        } catch (Throwable $exception) {
            $error = "「{$msg->taskName}」「{$key}」 {$msg->taskId}" . ExceptionHelper::dumpExceptionToString($exception);
            App::error($error);
            if (isset($target) && $target->alarm && null !== ($ding = ding($target->alarm))) {
                $ding->text($error);
            }
            $msg->deleteAllLock();
        }
    }
}
