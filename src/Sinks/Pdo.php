<?php


namespace Rabbit\Data\Pipeline\Sinks;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\activerecord\ActiveRecord;
use rabbit\core\Context;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\db\ConnectionInterface;
use rabbit\db\Exception;
use rabbit\db\MakePdoConnection;
use rabbit\db\mysql\CreateExt;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;

/**
 * Class Pdo
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Pdo extends AbstractPlugin
{
    /** @var string */
    protected $tableName;
    /** @var string */
    protected $dbName;

    /**
     * @param string $class
     * @param string $dsn
     * @param array $pool
     * @throws DependencyException
     * @throws NotFoundException
     * @throws Exception
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
            null,
            [10, 13, 0, 3]
        );
        MakePdoConnection::addConnection($class, $this->dbName, $dsn, $poolConfig);
    }

    /**
     * @return mixed|void
     * @throws Exception
     */
    public function init()
    {
        parent::init();
        [
            $class,
            $dsn,
            $pool
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['class', 'dsn', 'pool'],
            null,
            [
                'pool' => [],
            ]
        );
        if ($dsn === null || $class === null) {
            throw new InvalidConfigException("class, dsn must be set in $this->key");
        }
        $this->dbName = md5($dsn);
        $this->createConnection($class, $dsn, $pool);
    }

    /**
     * @param null $input
     * @param array $opt
     * @throws Exception
     */
    public function input(&$input = null, &$opt = [])
    {
        $model = new class($this->tableName, $this->dbName) extends ActiveRecord {
            /**
             *  constructor.
             * @param string $tableName
             * @param string $dbName
             */
            public function __construct(string $tableName, string $dbName)
            {
                Context::set(__CLASS__ . 'tableName', $tableName);
                Context::set(__CLASS__ . 'dbName', $tableName);
            }

            /**
             * @return mixed|string
             */
            public static function tableName()
            {
                return Context::get(__CLASS__ . 'tableName');
            }

            /**
             * @return ConnectionInterface
             */
            public static function getDb(): ConnectionInterface
            {
                return getDI('db')->getConnection(Context::get(__CLASS__ . 'dbName'));
            }
        };

        if (!CreateExt::create($model, $input)) {
            throw new Exception("save to " . $model::tableName() . ' failed!');
        }

        $this->output($model->toArray());
    }
}
