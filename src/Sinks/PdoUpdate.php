<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\ActiveRecord\ActiveRecord;
use Rabbit\Base\App;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\MakePdoConnection;
use Rabbit\Pool\ConnectionInterface;
use Throwable;

/**
 * Class PdoUpdate
 * @package Rabbit\Data\Pipeline\Sinks
 */
class PdoUpdate extends AbstractPlugin
{
    protected ?string $tableName;
    protected string $dbName;

    /**
     * @param string $class
     * @param string $dsn
     * @param array $pool
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Throwable
     */
    private function createConnection(string $class, string $dsn, array $pool): void
    {
        [
            $poolConfig['min'],
            $poolConfig['max'],
            $poolConfig['wait'],
            $poolConfig['retry']
        ] = ArrayHelper::getValueByArray(
            $pool,
            ['min', 'max', 'wait', 'retry'],
            [10, 12, 0, 3]
        );
        MakePdoConnection::addConnection($class, $this->dbName, $dsn, $poolConfig);
    }

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
        [
            $class,
            $dsn,
            $pool,
            $this->tableName
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'pool', 'tableName'],
            null,
            [
                'pool' => [],
            ]
        );
        if ($dsn === null || $class === null || $this->taskName === null) {
            throw new InvalidConfigException("class, dsn must be set in $this->key");
        }
        $this->dbName = md5($dsn);
        $this->createConnection($class, $dsn, $pool);
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        [
            $condition,
            $updates
        ] = ArrayHelper::getValueByArray($msg->data, ['where', 'updates']);
        if (empty($updates)) {
            throw new InvalidArgumentException("updates can not empty");
        }
        $model = new class($this->tableName, $this->dbName) extends ActiveRecord {
            /**
             *  constructor.
             * @param string $tableName
             * @param string $dbName
             */
            public function __construct(string $tableName, string $dbName)
            {
                Context::set(md5(get_called_class() . 'tableName'), $tableName);
                Context::set(md5(get_called_class() . 'dbName'), $dbName);
            }

            /**
             * @return mixed|string
             */
            public static function tableName(): string
            {
                return Context::get(md5(get_called_class() . 'tableName'));
            }

            /**
             * @return ConnectionInterface
             */
            public static function getDb(): ConnectionInterface
            {
                return getDI('db')->get(Context::get(md5(get_called_class() . 'dbName')));
            }
        };
        $msg->data = $model::updateAll($updates, $condition);
        if (!$msg->data) {
            App::warning("$this->tableName update failed");
        }
        $this->sink($msg);
    }
}
