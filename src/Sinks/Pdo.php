<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Base\App;
use Rabbit\DB\Exception;
use Rabbit\DB\MakePdoConnection;
use Rabbit\ActiveRecord\ARHelper;
use Rabbit\Data\Pipeline\Message;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\ActiveRecord\BaseActiveRecord;
use Rabbit\ActiveRecord\BaseRecord;
use Rabbit\Base\Exception\InvalidConfigException;

class Pdo extends AbstractPlugin
{
    protected ?string $tableName;
    protected string $dbName;
    protected string $func;
    protected int $retry = 1;
    protected ?int $sleep = null;
    protected array $retryCode = [];

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

    public function init(): void
    {
        parent::init();
        [
            $this->dbName,
            $class,
            $dsn,
            $pool,
            $this->tableName,
            $this->func,
            $this->retry,
            $this->retryCode,
            $this->sleep
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['dbName', 'class', 'dsn', 'pool', 'tableName', 'func', 'retry', 'retryCode', 'sleep'],
            [
                'pool' => [],
                'func' => 'create',
                'retry' => 1,
                'retryCode' => [],
            ]
        );
        if ($this->tableName === null) {
            throw new InvalidConfigException("taskName must be set in $this->key");
        }
        if (!in_array($this->func, ['create', 'update'])) {
            throw new InvalidConfigException("func only both set create or update");
        }
        if (!$this->dbName) {
            if ($dsn === null || $class === null) {
                throw new InvalidConfigException("when not set dbName then dsn & class must be set in $this->key");
            }
            $this->dbName = md5($dsn);
            $this->createConnection($class, $dsn, $pool);
        }
    }

    public function run(Message $msg): void
    {
        [
            $condition,
            $updates
        ] = ArrayHelper::getValueByArray($msg->data, ['where', 'updates'], ['where' => []]);

        $retry = $this->retry;
        while ($retry--) {
            try {
                if (isset($msg->data['columns'])) {
                    $this->saveWithLine($msg);
                } elseif ($updates) {
                    $this->saveWithCondition($msg, $updates, $condition);
                } else {
                    $this->saveWithModel($msg);
                }
                return;
            } catch (Exception $e) {
                $errorInfo = $e->errorInfo;
                if (!empty($errorInfo) &&  in_array($errorInfo[1], $this->retryCode)) {
                    App::error("Error code=$errorInfo[1],waiting & retry...");
                    $this->sleep && sleep($this->sleep);
                    continue;
                }
                throw $e;
            }
        }
    }

    protected function saveWithLine(Message $msg): void
    {
        $db = service('db')->get($this->dbName);
        $msg->data = $db->createCommand()->batchInsert($this->tableName, $msg->data['columns'], $msg->data['data'])->execute();
        $this->sink($msg);
    }

    protected function saveWithCondition(Message $msg, array $updates, array $condition): void
    {
        $model = $this->getModel($msg);
        $msg->data = $model->updateAll($updates, $condition);
        if (!$msg->data) {
            App::warning("$this->tableName update failed");
        }
        $this->sink($msg);
    }

    protected function saveWithModel(Message $msg): void
    {
        $model = $this->getModel($msg);
        $func = $this->func;
        $msg->data = ARHelper::$func($model, $msg->data);
        if (empty($msg->data)) {
            throw new Exception("save to " . $model->tableName() . ' failed!');
        }
        $this->sink($msg);
    }

    protected function getModel(Message $msg): BaseActiveRecord
    {
        if (isset($msg->opt['model'])) {
            if (is_string($msg->opt['model'])) {
                return new $msg->opt['model']();
            } elseif ($msg->opt['model'] instanceof BaseActiveRecord) {
                return clone $msg->opt['model'];
            }
        }
        $tableName = $msg->opt['tableName'] ?? $this->tableName;
        $dbname = $msg->opt['dbName'] ?? $this->dbName;
        return BaseRecord::build($tableName, $dbname);
    }
}
