<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\activerecord\ActiveRecord;
use rabbit\core\Context;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\db\Connection;
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
class PdoSave extends AbstractPlugin
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
            [42, 64, 0, 3]
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
        if (empty($this->tableName) && isset($this->opt['tableName'])) {
            $this->tableName = $this->opt['tableName'];
        }
        if (isset($this->input['columns'])) {
            $this->saveWithLine();
        } else {
            $this->saveWithModel();
        }
    }

    /**
     * @throws Exception
     */
    protected function saveWithLine(): void
    {
        /** @var Connection $db */
        $db = getDI('db')->getConnection($this->dbName);
        $res = $db->createCommand()->batchInsert($this->tableName, $this->input['columns'], $this->input['data'])->execute();
        $this->output($res);
    }

    /**
     * @throws Exception
     */
    protected function saveWithModel(): void
    {
        $model = new class($this->tableName, $this->dbName) extends ActiveRecord
        {
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
            public static function tableName()
            {
                return Context::get(md5(get_called_class() . 'tableName'));
            }

            /**
             * @return ConnectionInterface
             */
            public static function getDb(): ConnectionInterface
            {
                return getDI('db')->getConnection(Context::get(md5(get_called_class() . 'dbName')));
            }
        };

        $res = CreateExt::create($model, $this->input);
        if (empty($res)) {
            throw new Exception("save to " . $model::tableName() . ' failed!');
        }
        $this->output($res);
    }
}
