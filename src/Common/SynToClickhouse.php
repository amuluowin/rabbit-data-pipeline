<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\Message;

class SynToClickhouse extends BaseSyncData
{

    protected ?string $updatedAt;

    public function init(): void
    {
        parent::init();
        [
            $this->updatedAt,
            $this->db,
        ] = ArrayHelper::getValueByArray($this->config, ['updatedAt', 'db'], ['db' => 'click']);

        if ($this->primary === null && $this->updatedAt === null) {
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
            if (str_ends_with($key, ')')) {
                $func = substr($key, 0, strpos($key, '('));
                $params = explode(',', substr($key, strpos($key, '(') + 1, strpos($key, ')') - strpos($key, '(') - 1));
                $params[0] = "f.{$params[0]}";
                $str = implode(',', $params);
                $on .= "$func({$str})";
                $str = str_replace('f.', 'k.', $str);
                $on .= " = $func({$str}) and ";
            } else {
                $on .= "f.$key=t.$key and ";
            }
        }
        $on = rtrim($on, ' and ');

        if ($this->updatedAt !== null) {
            $sql = "INSERT INTO {$this->to} ({$this->field}" . ($this->onlyInsert ? ')' : ',flag)') . "
            SELECT {$fields}" . ($this->onlyInsert ? '' : ',0 AS flag') . "
            FROM {$this->from} f where f.{$this->updatedAt} > (SELECT max({$this->updatedAt}) from {$this->to} )";
        } else {
            $sql = "INSERT INTO {$this->to} ({$this->field}" . ($this->onlyInsert ? ')' : ',flag)') . "
            SELECT {$fields}" . ($this->onlyInsert ? '' : ',0 AS flag') . "
              FROM {$this->from} f 
              ANTI LEFT JOIN {$this->to} t on $on" . ($this->onlyInsert ? '' : "
             WHERE ({$primary}) NOT IN (
            SELECT {$this->primary} FROM {$this->to}
             WHERE flag = 0)");
        }


        getDI('db')->get($this->db)->createCommand($sql)->execute();

        if (!$this->onlyInsert) {
            $sql = "ALTER TABLE {$this->to}
            UPDATE flag = flag + 1
             WHERE {$this->primary}  in (
            SELECT {$this->primary}
              FROM {$this->to}
             WHERE flag = 0)  and flag in (0, 1)";
            $msg->data = getDI('db')->get($this->db)->createCommand($sql)->execute();
        }

        $this->sink($msg);
    }
}
