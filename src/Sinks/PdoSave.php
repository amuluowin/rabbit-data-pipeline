<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\ActiveRecord\ActiveRecord;
use Rabbit\ActiveRecord\ARHelper;
use Rabbit\Base\Core\Context;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Exception\NotSupportedException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\ConnectionInterface;
use Rabbit\DB\Exception;
use Rabbit\DB\MakePdoConnection;
use Throwable;

/**
 * Class Pdo
 * @package Rabbit\Data\Pipeline\Sinks
 */
class PdoSave extends AbstractPlugin
{
    /** @var string */
    protected ?string $tableName;
    /** @var string */
    protected string $dbName;
    /** @var string */
    protected string $driver = 'db';

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
     * @throws NotFoundException
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $mysql,
            $class,
            $dsn,
            $pool,
            $this->tableName
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['mysql', 'class', 'dsn', 'pool', 'tableName'],
            [
                'pool' => [],
            ]
        );
        if ($this->taskName === null) {
            throw new InvalidConfigException("taskName must be set in $this->key");
        }
        if ($mysql === null && ($dsn === null || $class === null)) {
            throw new InvalidConfigException("$this->key must need prams: mysql or class, dsn");
        }
        if (!empty($mysql)) {
            [$this->driver, $this->dbName] = explode('.', trim($mysql));
        } else {
            $this->dbName = md5($dsn);
            $this->createConnection($class, $dsn, $pool);
        }
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        if (empty($this->tableName) && isset($msg->opt['tableName'])) {
            $this->tableName = $msg->opt['tableName'];
        }
        if (isset($msg->data['columns'])) {
            $this->saveWithLine();
        } else {
            $this->saveWithModel();
        }
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    protected function saveWithLine(Message $msg): void
    {
        $db = getDI($this->driver)->get($this->dbName);
        $msg->data = $db->createCommand()->batchInsert($this->tableName, $msg->data['columns'], $msg->data['data'])->execute();
        $this->sink($msg);
    }

    /**
     * @param Message $msg
     * @throws Exception
     * @throws Throwable
     * @throws NotSupportedException
     */
    protected function saveWithModel(Message $msg): void
    {
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

        $msg->data = ARHelper::create($model, $msg->data);
        if (empty($res)) {
            throw new Exception("save to " . $model::tableName() . ' failed!');
        }
        $this->sink($res);
    }
}
