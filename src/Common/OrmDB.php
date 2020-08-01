<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;


use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Data\Pipeline\Sources\Pdo;
use Throwable;

/**
 * Class OrmDB
 * @package Rabbit\Data\Pipeline\Common
 */
class OrmDB extends Pdo
{
    /**
     * @return mixed|void
     * @throws DependencyException
     * @throws InvalidConfigException
     * @throws NotFoundException
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        if (!is_array($this->sql)) {
            throw new InvalidConfigException("sql only support array");
        }
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        $params = $this->makeParams($msg);
        $msg->data = getDI('db')->get($this->dbName)->createCommandExt($this->sql, $params)->cache($this->duration, $this->cache->getDriver($this->cacheDriver))->{$this->query}();
        $this->send($msg);
    }
}