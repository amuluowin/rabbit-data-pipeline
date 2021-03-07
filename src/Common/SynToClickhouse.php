<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\Message;

class SynToClickhouse extends BaseSyncData
{
    protected ?string $equal;
    protected string $primary;

    public function init(): void
    {
        parent::init();
        [
            $this->equal,
            $this->primary
        ] = ArrayHelper::getValueByArray($this->config, ['equal', 'primary']);

        if ($this->primary === null) {
            throw new InvalidConfigException('primary field is empty!');
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
        $sql = "INSERT INTO {$this->to}
        SELECT {$fields},0 AS flag
          FROM {$this->from} f 
          ANTI LEFT JOIN {$this->to} t on $on
         WHERE ({$primary}) NOT IN(
        SELECT {$this->primary} FROM {$this->to}
         WHERE flag= 0)
                ";
        getDI('click')->get($this->db)->createCommand($sql)->execute();


        $sql = "ALTER TABLE {$this->to}
UPDATE flag= flag+ 1
 WHERE {$this->primary}  in(
SELECT {$this->primary}
  FROM {$this->to}
 WHERE flag= 0)  and flag in(0, 1)";
        $msg->data = getDI('click')->get($this->db)->createCommand($sql)->execute();
        $this->sink($msg);
    }
}
