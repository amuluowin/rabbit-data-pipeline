<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use DI\DependencyException;
use DI\NotFoundException;
use Generator;
use Psr\SimpleCache\CacheInterface;
use Rabbit\Base\App;
use Rabbit\Base\Contract\InitInterface;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use ReflectionException;
use Throwable;
use function Swoole\Coroutine\batch;

/**
 * Interface AbstractPlugin
 * @package Rabbit\Data\Pipeline
 * @property Scheduler scheduler
 */
abstract class AbstractPlugin extends BaseObject implements InitInterface
{
    public string $taskName;
    public string $key;
    protected array $config = [];
    public array $locks = [];
    public array $output = [];
    protected bool $start = false;
    protected ?CacheInterface $cache;
    const CACHE_KEY = 'cache';
    protected ?array $errHandler;
    protected ?array $lockKey = [];
    protected array $inPlugin = [];
    protected string $scName;
    protected bool $wait = false;

    /**
     * AbstractPlugin constructor.
     * @param string $scName
     * @param array $config
     */
    public function __construct(string $scName, array $config)
    {
        $this->config = $config;
        $this->scName = $scName;
    }

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
    {
        $this->cache = getDI(self::CACHE_KEY);
        $this->errHandler = ArrayHelper::getValue($this->config, 'errHandler');
        $this->lockKey = ArrayHelper::getValue($this->config, 'lockKey', []);
    }

    /**
     * @return SchedulerInterface
     * @throws Throwable
     */
    public function getScheduler(): SchedulerInterface
    {
        return getDI($this->scName);
    }

    /**
     * @return bool
     */
    public function getStart(): bool
    {
        return $this->start;
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function process(Message $msg): void
    {
        try {
            $data = $this->run($msg);
            if ($data instanceof Generator) {
                if ($this->wait) {
                    $batch = [];
                    foreach ($data as $input) {
                        $tmp = clone $msg;
                        $tmp->data = $input;
                        $batch[] = function () use ($tmp) {
                            return $this->sink($tmp);
                        };
                    }
                    batch($batch);
                } else {
                    foreach ($data as $input) {
                        $tmp = clone $msg;
                        $tmp->data = $input;
                        rgo(function () use ($tmp) {
                            $this->sink($tmp);
                        });
                    }
                }
            } elseif ($data instanceof Message) {
                $this->sink($msg);
            } elseif ($data !== null) {
                $msg->data = $data;
                $this->sink($msg);
            }
        } catch (Throwable $exception) {
            if (empty($this->errHandler)) {
                throw $exception;
            }
            if (!is_array($this->errHandler)) {
                $this->errHandler = [$this->errHandler];
            }
            //删除锁
            $msg->deleteAllLock();

            $errHandler = $this->errHandler;
            $this->dealException($errHandler, $exception);
        }
    }

    /**
     * @param array $errHandler
     * @param Throwable $exception
     * @throws Throwable
     */
    public function dealException(array &$errHandler, Throwable $exception)
    {
        while (!empty($errHandler)) {
            try {
                $handle = array_shift($errHandler);
                if (is_callable($handle)) {
                    call_user_func($handle, $this, $exception);
                } else {
                    throw $exception;
                }
            } catch (Throwable $exception) {
                throw $exception;
            }
        }
    }

    abstract public function run(Message $msg);

    /**
     * @param Message $msg
     * @throws Throwable
     * @throws DependencyException
     * @throws NotFoundException
     * @throws InvalidConfigException
     * @throws ReflectionException
     */
    protected function sink(Message $msg): void
    {
        foreach ($this->output as $output => $transfer) {
            if ($transfer === false) {
                if (!isset($this->inPlugin[$output])) {
                    $plugin = $this->scheduler->getTarget($this->taskName, $output);
                    $this->inPlugin[$output] = $plugin;
                } else {
                    $plugin = $this->inPlugin[$output];
                }
                $plugin->process($msg);
                return;
            }
            if (empty($msg->data)) {
                App::warning("「{$this->taskName}」 $this->key -> $output; data is empty", 'Data');
            } else {
                App::info("「{$this->taskName}」 $this->key -> $output;", 'Data');
            }
            $this->getScheduler()->next($msg, $output);
        }
    }
}
