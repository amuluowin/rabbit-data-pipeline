<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\activerecord\ActiveRecord;
use rabbit\App;
use rabbit\core\Context;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\db\ConnectionInterface;
use rabbit\db\Exception;
use rabbit\db\MakePdoConnection;
use rabbit\exception\InvalidArgumentException;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;

/**
 * Class PdoUpdate
 * @package Rabbit\Data\Pipeline\Sinks
 */
class PdoUpdate extends AbstractPlugin
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
            [10, 12, 0, 3]
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
     * @throws Exception
     */
    public function run(): void
    {
        [
            $condition,
            $updates
        ] = ArrayHelper::getValueByArray($this->input, ['where', 'updates']);
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
                parent::__construct();
            }

            /**
             * @return mixed|string
             */
            public static function tableName()
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
        $res = $model::updateAll($updates, $condition);
        if (!$res) {
            App::warning("$this->tableName update failed");
        }
        $this->output($res);
    }
}
