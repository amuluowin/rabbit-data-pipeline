<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Rabbit\Base\App;
use Rabbit\Base\Core\BaseObject;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\InvalidCallException;
use Rabbit\Base\Helper\ArrayHelper;
use Throwable;

/**
 * Class Message
 * @package Rabbit\Data\Pipeline
 */
class Message extends BaseObject
{
    public $data;
    public string $taskName;
    public string $taskId;
    public array $opt = [];
    public array $request = [];
    protected string $redisKey = 'default';

    /**
     * @author Albert <63851587@qq.com>
     * @param array $configs
     */
    public function __construct(array $configs = [])
    {
        \configure($this, $configs);
    }

    public function __clone()
    {
        $this->data = null;
    }

    /**
     * @param string $key
     * @return mixed|null
     */
    public function getFromInput(string $key)
    {
        return ArrayHelper::getValue($this->data, $key);
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
     * @param $key
     * @return string
     * @throws Exception
     */
    public function makeLockKey(string $key): string
    {
        is_array($key) && $key = implode('_', $key);
        if (!is_string($key)) {
            throw new Exception("lockKey Must be string or array");
        }
        return 'Locks:' . $key;
    }

    /**
     * @param string|null $key
     * @param float|null $ext
     * @return bool
     * @throws Throwable
     */
    public function getLock(string $key = null, float $ext = 60): bool
    {
        $key ?: $this->taskId;
        if ((bool)getDI('redis')->get($this->redisKey)->set($key, true, ['NX', 'EX' => $ext])) {
            $this->opt['Locks'][] = $key;
            return true;
        }
        return false;
    }

    /**
     * @throws Throwable
     */
    public function deleteAllLock(): void
    {
        $locks = isset($this->opt['Locks']) ? $this->opt['Locks'] : [];
        foreach ($locks as $lock) {
            !is_string($lock) && $lock = strval($lock);
            $this->deleteLock($lock);
        }
    }

    /**
     * @param string|null $key
     * @return int
     * @throws Throwable
     */
    public function deleteLock(string $key = null): int
    {
        $key ?: $this->taskId;
        if ($flag = getDI('redis')->get($this->redisKey)->del($key)) {
            App::warning("「{$this->taskName}」 Delete Lock: " . $key);
        }
        return (int)$flag;
    }

    /**
     * @param string $key
     * @param $item
     */
    public function makeOptions(string $key, $item): void
    {
        if (is_array($item)) {
            [$method, $params] = ArrayHelper::getValueByArray($item, ['method', 'params'], null, ['params' => []]);
            if (empty($method)) {
                throw new InvalidArgumentException("method must be set!");
            }
            if (!is_callable($method)) {
                throw new InvalidCallException("$method does not exists");
            }
            call_user_func_array($method, [$key, $params, $this]);
        }
        if (is_string($item)) {
            if (strtolower($item) === 'input') {
                $data[$key] = $this->data;
            } elseif (strtolower($item) === 'opt') {
                $data[$key] = $this->opt;
            } else {
                $pos = strpos($item, '.') ? strpos($item, '.') : strlen($item);
                $from = strtolower(substr($item, 0, $pos));
                switch ($from) {
                    case 'input':
                        $data[$key] = ArrayHelper::getValue($this->data, substr($item, $pos + 1));
                        break;
                    case 'opt':
                        $data[$key] = ArrayHelper::getValue($this->opt, substr($item, $pos + 1));
                        break;
                    default:
                        $data[$key] = $item;
                }
            }
        }
    }
}
