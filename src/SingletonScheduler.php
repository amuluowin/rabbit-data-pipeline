<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use rabbit\App;
use rabbit\helper\ExceptionHelper;

/**
 * Class SingletonScheduler
 * @package Rabbit\Data\Pipeline
 */
class SingletonScheduler extends Scheduler
{
    /** @var string */
    protected $name = 'singletonscheduler';

    /**
     * @param string $task
     * @param array|null $params
     */
    public function process(string $task, array $params = []): void
    {
        /** @var AbstractSingletonPlugin $target */
        foreach ($this->targets[$task] as $target) {
            if ($target->getStart()) {
                $target->task_id = (string)getDI('idGen')->create();
                $opt = [];
                $input = [];
                $target->process($input, $opt, $params);
            }
        }
    }

    /**
     * @param string $taskName
     * @param string $key
     * @param string|null $task_id
     * @param $data
     * @param bool $transfer
     * @param array $opt
     * @throws Exception
     */
    public function send(string $taskName, string $key, ?string $task_id, &$data, ?int $transfer, array $opt = [], array $request = []): void
    {
        try {
            /** @var AbstractSingletonPlugin $target */
            $target = $this->targets[$taskName][$key];
            if (empty($data)) {
                App::warning("$taskName $key input empty data,ignore!");
                $this->redis->del($task_id);
                return;
            }

            if ($transfer === null) {
                $target->process($data, $opt);
            } else {
                $this->transSend($taskName, $key, $task_id, $data, $transfer, $opt);
            }
        } catch (\Throwable $exception) {
            App::error(ExceptionHelper::dumpExceptionToString($exception));
            $this->redis->del($task_id);
        }
    }
}
