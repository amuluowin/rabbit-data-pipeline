<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Exception;
use Throwable;
use Rabbit\Base\App;
use Rabbit\Cron\CronJob;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Cron\CronExpression;

class Scheduler implements SchedulerInterface
{
    protected array $targets = [];
    protected string $name = 'scheduler';
    protected array $config = [];
    protected array $senders = [];
    protected string $redisKey = 'default';
    protected ?CronJob $cron = null;
    protected array $loops = [];

    public function __construct(protected readonly ConfigParserInterface $parser)
    {
        $this->config = $this->parser->parse();
    }

    public function getConfig(): array
    {
        return $this->config;
    }

    public function run(string $key, string $target = null, array $params = []): array
    {
        $taskResult = [];
        if (isset($this->config[$key])) {
            try {
                if ($target && isset($this->config[$key][$target])) {
                    $runTarget = $this->getTarget($key, $target);
                    $msg = create(Message::class, ['redis' => service('redis')->get($this->redisKey), ...$params], false);
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

    public function multi(array $tasks, int $wait = -1, array $params = []): array
    {
        $taskResult = [];
        wgeach($tasks, function (int $i, string $key) use (&$taskResult, $params): void {
            $taskResult = [...$taskResult, ...$this->run($key, null, $params)];
        }, $wait);
        return $taskResult;
    }

    private function start(string $key, array &$params): string
    {
        $result = '';
        $lock = ArrayHelper::getValue($this->config[$key], 'lock');
        $expression = (string)ArrayHelper::getValue($this->config[$key], 'cron');
        $func = function (string $key, string $expression, array &$params): void {
            if ($this->cron && $expression !== '') {
                if (is_numeric($expression)) {
                    if ((int)$expression >= 0) {
                        if (!isset($this->loops[$key])) {
                            $this->loops[$key] = loop(function () use ($key, &$params, $expression): void {
                                $this->process($key, $params);
                                App::info("$key finished once! Go on with {$expression}s later");
                            }, (int)$expression * 1000);
                        }
                    } else {
                        $this->process($key, $params);
                    }
                } else {
                    $this->cron->add($key, [$expression, function () use ($key, &$params): void {
                        $this->process($key, $params);
                    }]);
                    $this->cron->run($key);
                    App::info("$key run with cron: $expression");
                }
            } else {
                $this->process($key, $params);
            }
        };
        if ($lock && false === rlock($this->name . '.' . $key, function () use ($func, $key, $expression, &$params): void {
            $func($key, $expression, $params);
        }, false, $lock)) {
            App::warning("$key is running");
            $result = "$key is running";
        } else {
            $result = "$key start run";
            $func($key, $expression, $params);
        }

        return $result;
    }

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

    public function process(string $task, array $params = []): void
    {
        /** @var AbstractPlugin $target */
        foreach ($this->config[$task] as $key => $tmp) {
            if ($key === 'lock') {
                continue;
            }
            if (is_array($tmp) && ArrayHelper::getValue($tmp, 'start') === true) {
                $target = $this->getTarget($task, $key);
                $msg = create(Message::class, ['redisKey' => $this->redisKey, 'taskName' => $task, 'taskId' => (string)service('idGen')->nextId()], false);
                $target->process($msg);
            }
        }
    }

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
