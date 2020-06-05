<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use rabbit\App;
use rabbit\contract\InitInterface;
use rabbit\core\Context;
use rabbit\helper\ArrayHelper;
use rabbit\helper\VarDumper;

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
     * @param array $opt
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
     * @return int
     */
    public function getLock(string $key = null,$ext = null): bool
    {
        empty($ext) && $ext = $this->lockEx;
        if (($key || $key = $this->getTaskId()) && $this->scheduler->getLock($key,$this->lockEx)) {
            $this->getOpt()['Locks'][] = $key;
            return true;
        }
        return false;
    }

    /**
     * @param string $lockKey
     * @return bool
     */
    public function deleteLock(string $key = null): int
    {
        ($key === null) && $key = $this->getTaskId();
        return $this->scheduler->deleteLock($this->taskName, $key);
    }

    public function deleteAllLock(): void
    {
        $this->scheduler->deleteAllLock($this->getOpt());
    }

    /**
     * @param $data
     * @throws Exception
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
                $plugin->setOpt($this->getOpt());
                $plugin->setRequest($this->getRequest());
                $plugin->process();
                return;
            }
            if (empty($data)) {
                App::warning("「{$this->taskName}」 $this->key -> $output; data is empty", 'Data');
            } elseif ($this->logInfo === self::LOG_SIMPLE) {
                App::info("「{$this->taskName}」 $this->key -> $output;", 'Data');
            } else {
                App::info("「{$this->taskName}」 $this->key -> $output; data: " . VarDumper::getDumper()->dumpAsString($data), 'Data');
            }
            $this->scheduler->send($this->taskName, $output, $this->getTaskId(), $data, (bool)$transfer, $this->getOpt(), $this->getRequest(), $this->wait);
        }
    }
}
