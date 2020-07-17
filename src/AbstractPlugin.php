<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Closure;
use Exception;
use Psr\SimpleCache\CacheInterface;
use Rabbit\Base\App;
use Rabbit\Base\Contract\InitInterface;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\InvalidCallException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Throwable;

/**
 * Interface AbstractPlugin
 * @package Rabbit\Data\Pipeline
 * @property Scheduler scheduler
 * @property array opt
 * @property mixed input
 * @property array request
 */
abstract class AbstractPlugin extends BaseObject implements InitInterface
{
    /** @var string */
    public string $taskName;
    /** @var string */
    private string $taskId;
    /** @var string */
    public string $key;
    /** @var array */
    protected array $config = [];
    /** @var mixed */
    private $input;
    /** @var array */
    private array $request = [];
    /** @var array */
    private array $opt = [];
    /** @var array */
    public array $locks = [];
    /** @var array */
    protected array $output = [];
    /** @var bool */
    protected bool $start = false;
    /** @var int */
    protected int $lockEx = 0;
    /** @var CacheInterface */
    protected ?CacheInterface $cache;
    /** @var string */
    const CACHE_KEY = 'cache';
    /** @var string */
    const LOCK_KEY = 'Plugin';
    /** @var array */
    protected ?array $errHandler;
    /** @var bool */
    protected bool $wait = false;
    /** @var string */
    protected string $pluginName;
    /** @var array */
    protected ?array $lockKey = [];
    /** @var AbstractPlugin[] */
    protected array $inPlugin = [];
    /** @var string */
    protected string $scName;

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
     * @param $name
     * @return mixed|null
     */
    public function &__get($name)
    {
        if (property_exists($this, $name)) {
            return $this->$name;
        }
        $getter = 'get' . $name;
        if (method_exists($this, $getter)) {
            $value = $this->$getter();
            return $value;
        }
        $res = null;
        return $res;
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
     * @return string
     */
    public function getTaskId(): string
    {
        return $this->taskId;
    }

    /**
     * @param string $taskId
     */
    public function setTaskId(string $taskId): void
    {
        $this->taskId = $taskId;
    }

    /**
     * @return array
     */
    public function getRequest(): array
    {
        return $this->request;
    }

    /**
     * @param array $request
     */
    public function setRequest(array &$request): void
    {
        $this->request = $request;
    }

    public function getInput()
    {
        return $this->input;
    }

    /**
     * @param $input
     */
    public function setInput(&$input)
    {
        $this->input = $input;
    }

    /**
     * @return array
     */
    public function getOpt(): array
    {
        return $this->opt;
    }

    /**
     * @param array $opt
     */
    public function setOpt(array &$opt): void
    {
        $this->opt = $opt;
    }

    /**
     * @return bool
     */
    public function getStart(): bool
    {
        return $this->start;
    }

    /**
     * @param string|null $key
     * @param int $ext
     * @return bool
     */
    public function getLock(string $key = null, int $ext = null): bool
    {
        empty($ext) && $ext = $this->lockEx;
        if (($key || $key = $this->taskId) && $this->scheduler->getLock($key, $ext)) {
            $this->opt['Locks'][] = $key;
            return true;
        }
        return false;
    }

    /**
     * @param $key
     * @return string
     * @throws Exception
     */
    public function makeLockKey($key): string
    {
        is_array($key) && $key = implode('_', $key);
        if (!is_string($key)) {
            throw new Exception("lockKey Must be string or array");
        }
        return 'Locks:' . $key;
    }

    public function deleteAllLock(): void
    {
        $this->scheduler->deleteAllLock($this->taskName, $this->opt);
    }

    /**
     * @param string|null $key
     * @return int
     * @throws Throwable
     */
    public function deleteLock(string $key = null): int
    {
        ($key === null) && $key = $this->taskId;
        return $this->scheduler->deleteLock($this->taskName, $key);
    }

    /**
     * @param string $key
     * @param Closure $function
     * @param array $params
     * @return mixed|null
     * @throws Throwable
     */
    public function redisLock(string $key, Closure $function, array $params)
    {
        try {
            if ($this->scheduler->redis->setnx($key, true)) {
                return call_user_func_array($function, $params);
            }
            return null;
        } catch (Throwable $exception) {
            App::error(ExceptionHelper::dumpExceptionToString($exception));
            return null;
        } finally {
            $this->scheduler->redis->del($key);
        }
    }

    /**
     * @param string $key
     * @return mixed|null
     */
    public function getFromInput(string $key)
    {
        return ArrayHelper::getValue($this->input, $key);
    }

    /**
     * @param string $key
     * @return mixed|null
     */
    public function getFromOpt(string $key)
    {
        return ArrayHelper::getValue($this->opt, $key);
    }

    /**
     * @param array $data
     * @param array $input
     * @param array $opt
     * @param string $key
     * @param $item
     */
    public function makeOptions(array &$data, array &$input, array &$opt, string $key, $item): void
    {
        if (is_array($item)) {
            [$method, $params] = ArrayHelper::getValueByArray($item, ['method', 'params'], null, ['params' => []]);
            if (empty($method)) {
                throw new InvalidArgumentException("method must be set!");
            }
            if (!is_callable($method)) {
                throw new InvalidCallException("$method does not exists");
            }
            call_user_func_array($method, [$key, $params, &$input, &$opt, &$data]);
        }
        if (is_string($item)) {
            if (strtolower($item) === 'input') {
                $data[$key] = $input;
            } elseif (strtolower($item) === 'opt') {
                $data[$key] = $opt;
            } else {
                $pos = strpos($item, '.') ? strpos($item, '.') : strlen($item);
                $from = strtolower(substr($item, 0, $pos));
                switch ($from) {
                    case 'input':
                        $data[$key] = ArrayHelper::getValue($input, substr($item, $pos + 1));
                        break;
                    case 'opt':
                        $data[$key] = ArrayHelper::getValue($opt, substr($item, $pos + 1));
                        break;
                    default:
                        $data[$key] = $item;
                }
            }
        }
    }

    /**
     * @throws Throwable
     */
    public function process(): void
    {
        try {
            $this->run();
        } catch (Throwable $exception) {
            if (empty($this->errHandler)) {
                throw $exception;
            }
            if (!is_array($this->errHandler)) {
                $this->errHandler = [$this->errHandler];
            }
            //删除锁
            $this->deleteAllLock();

            $errerrHandler = $this->errHandler;
            self::dealException($errerrHandler, $exception);
        }
    }

    /**
     * @param array $errerrHandler
     * @param Throwable $exception
     * @throws Throwable
     */
    public function dealException(array &$errerrHandler, Throwable $exception)
    {
        while (!empty($errerrHandler)) {
            try {
                $handle = array_shift($errerrHandler);
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

    abstract public function run();

    /**
     * @param $data
     * @throws Throwable
     */
    public function output(&$data): void
    {
        foreach ($this->output as $output => $transfer) {
            if ($transfer === 'wait') {
                if (!isset($this->inPlugin[$output])) {
                    $plugin = $this->scheduler->getTarget($this->taskName, $output);
                    $this->inPlugin[$output] = $plugin;
                } else {
                    $plugin = $this->inPlugin[$output];
                }
                $plugin->taskId = $this->taskId;
                $plugin->input = &$data;
                $plugin->opt = &$this->opt;
                $plugin->request = &$this->request;
                $plugin->process();
                return;
            }
            if (empty($data)) {
                App::warning("「{$this->taskName}」 $this->key -> $output; data is empty", 'Data');
            } else {
                App::info("「{$this->taskName}」 $this->key -> $output;", 'Data');
            }
            $this->getScheduler()->send($this, $output, $data, (bool)$transfer);
        }
    }
}
