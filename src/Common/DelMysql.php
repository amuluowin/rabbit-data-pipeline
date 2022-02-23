<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;

class DelMysql extends AbstractPlugin
{
    protected ?string $table = null;

    protected string $db = 'default';

    protected ?string $where = null;

    public function init(): void
    {
        parent::init();
        [
            $this->table,
            $this->db,
            $this->where
        ] = ArrayHelper::getValueByArray($this->config, ['table', 'db', 'where'], [$this->table, $this->db, $this->where]);

        if (empty($this->table) || empty($this->where)) {
            throw new InvalidArgumentException("hosts & where empty!");
        }
    }

    public function run(Message $msg): void
    {
        $db = service('db')->get($this->db);
        $sql = "delete from {$this->table} where {$this->where}";
        $db->createCommand($sql)->execute();
    }
}
