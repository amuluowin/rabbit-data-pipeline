<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DI\DependencyException;
use DI\NotFoundException;
use rabbit\App;
use Rabbit\Data\Pipeline\AbstractPlugin;
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
        $model = eval(sprintf("return new class() extends \\rabbit\\activerecord\\ActiveRecord {
        /**
         * @return mixed|string
         */
        public static function tableName()
        {
            return '%s';
        }

        /**
         * @return ConnectionInterface
         */
        public static function getDb(): \\rabbit\\db\\ConnectionInterface
        {
            return getDI('db')->getConnection('%s');
        }
    };", $this->tableName, $this->dbName));
        $res = $model::updateAll($updates, $condition);
        if (!$res) {
            App::warning("$this->tableName update failed");
        }
        $this->output($res);
    }
}
