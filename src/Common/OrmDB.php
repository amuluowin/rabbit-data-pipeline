<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;


use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
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
     * @throws Throwable
     */
    public function run(): void
    {
        $params = [];
        foreach ($this->params as $key => $value) {
            switch ($value) {
                case 'getFromInput':
                    $params[] = ArrayHelper::getValue($this->input, $key);
                    break;
                case 'input':
                    $params[] = json_encode($this->input, JSON_UNESCAPED_UNICODE);
                    break;
                default:
                    if (method_exists($this, $value)) {
                        $params[] = $this->$value();
                    } else {
                        $params[] = $value;
                    }
            }
        }
        $data = getDI('db')->get($this->dbName)->createCommandExt($this->sql, $params)->cache($this->duration, $this->cache->getDriver($this->cacheDriver))->{$this->query}();
        $this->send($data);
    }
}