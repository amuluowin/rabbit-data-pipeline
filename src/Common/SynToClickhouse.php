<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\Message;

class SynToClickhouse extends BaseSyncData
{

    protected string $updatedAt;

    public function init(): void
    {
        parent::init();
        [
            $this->updatedAt
        ] = ArrayHelper::getValueByArray($this->config, ['updatedAt']);

        if ($this->primary === null && $this->updatedAt) {
            throw new InvalidConfigException('primary & updatedAt both empty!');
        }
    }

    public function run(Message $msg): void
    {
        $primary = '';
        foreach (explode(',', $this->primary) as $key) {
            $primary .= "f.$key,";
        }
        $primary = rtrim($primary, ',');
        $fields = '';
        foreach (explode(',', $this->field) as $key) {
            $fields .= "f.$key,";
        }
        $fields = rtrim($fields, ',');

        $onAll = $this->equal ?? $this->field;
        $on = '';
        foreach (explode(',', $onAll) as $key) {
            $on .= "f.$key=t.$key and ";
        }
        $on = rtrim($on, ' and ');

        if ($this->updatedAt !== null) {
            $sql = "INSERT INTO {$this->to} ({$this->field}" . ($this->onlyInsert ? ')' : ',flag)') . "
            SELECT {$fields}" . ($this->onlyInsert ? '' : ',0 AS flag') . "
            FROM {$this->from} f where f.{$this->updatedAt}>(SELECT max({$this->updatedAt}) from {$this->to} )";
        } else {
            $sql = "INSERT INTO {$this->to} ({$this->field}" . ($this->onlyInsert ? ')' : ',flag)') . "
            SELECT {$fields}" . ($this->onlyInsert ? '' : ',0 AS flag') . "
              FROM {$this->from} f 
              ANTI LEFT JOIN {$this->to} t on $on" . ($this->onlyInsert ? '' : "
             WHERE ({$primary}) NOT IN(
            SELECT {$this->primary} FROM {$this->to}
             WHERE flag= 0)");
        }


        getDI('click')->get($this->db)->createCommand($sql)->execute();

        if (!$this->onlyInsert) {
            $sql = "ALTER TABLE {$this->to}
            UPDATE flag= flag+ 1
             WHERE {$this->primary}  in(
            SELECT {$this->primary}
              FROM {$this->to}
             WHERE flag= 0)  and flag in(0, 1)";
            $msg->data = getDI('click')->get($this->db)->createCommand($sql)->execute();
        }

        $this->sink($msg);
    }
}
