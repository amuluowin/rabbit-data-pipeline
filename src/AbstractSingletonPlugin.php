<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Exception;
use Rabbit\Base\App;
use Rabbit\Base\Contract\InitInterface;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Helper\ArrayHelper;
use Throwable;

/**
 * Class AbstractSingletonPlugin
 * @package Rabbit\Data\Pipeline
 */
abstract class AbstractSingletonPlugin extends AbstractPlugin implements InitInterface
{
    /**
     * @return string
     */
    public function getTaskId(): string
    {
        return (string)Context::get($this->taskName . $this->key . 'taskid');
    }

    /**
     * @param string $taskId
     */
    public function setTaskId(string $taskId): void
    {
        Context::set($this->taskName . $this->key . 'taskid', $taskId);
    }

    /**
     * @param string $key
     * @return mixed|null
     */
    public function getFromInput(string $key)
    {
        return ArrayHelper::getValue($this->getInput(), $key);
    }

    /**
     * @param string $key
     * @return mixed|null
     */
    public function getFromOpt(string $key)
    {
        return ArrayHelper::getValue($this->getOpt(), $key);
    }

    /**
     * @return array
     */
    public function getRequest(): array
    {
        return (array)Context::get($this->getTaskId() . 'request');
    }

    /**
     * @param array $request
     */
    public function setRequest(array &$request): void
    {
        Context::set($this->getTaskId() . 'request', $request);
    }

    public function getInput()
    {
        return Context::get($this->getTaskId() . 'input');
    }

    /**
     * @param $input
     */
    public function setInput(&$input)
    {
        Context::set($this->getTaskId() . 'input', $input);
    }

    /**
     * @return array
     */
    public function getOpt(): array
    {
        return (array)Context::get($this->getTaskId() . 'opt');
    }

    /**
     * @param array $opt
     */
    public function setOpt(array &$opt): void
    {
        Context::set($this->getTaskId() . 'opt', $opt);
    }

    /**
     * @param string|null $key
     * @param int $ext
     * @return bool
     */
    public function getLock(string $key = null, int $ext = null): bool
    {
        empty($ext) && $ext = $this->lockEx;
        if (($key || $key = $this->getTaskId()) && $this->scheduler->getLock($key, $this->lockEx)) {
            $this->getOpt()['Locks'][] = $key;
            return true;
        }
        return false;
    }

    /**
     * @param string|null $key
     * @return int
     * @throws Throwable
     */
    public function deleteLock(string $key = null): int
    {
        ($key === null) && $key = $this->getTaskId();
        return $this->scheduler->deleteLock($this->taskName, $key);
    }

    /**
     * @throws Throwable
     */
    public function deleteAllLock(): void
    {
        $this->scheduler->deleteAllLock($this->taskName, $this->getOpt());
    }

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
                $plugin->setTaskId($this->getTaskId());
                $plugin->setInput($data);
                $opt = $this->getOpt();
                $plugin->setOpt($opt);
                $req = $this->getRequest();
                $plugin->setRequest($req);
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
