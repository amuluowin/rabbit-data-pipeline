<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\App;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\Message;
use Throwable;

class SynToMysql extends BaseSyncData
{
    public function run(Message $msg): void
    {
        $updates = [];
        foreach (explode(',', $this->field) as $key) {
            $key = trim($key);
            $updates[] = "$key=values($key)";
        }
        if ($this->equal) {
            $equal = '';
            foreach (explode(',', $this->equal) as $key) {
                $equal .= "f.$key=t.$key and ";
            }
            $equal = rtrim($equal, ' and ');
            $sql = "INSERT INTO {$this->to} ({$this->field}) SELECT {$this->field} FROM {$this->from} f WHERE NOT EXISTS (SELECT 1 FROM {$this->to} t WHERE $equal)";
        } else {
            $sql = sprintf("INSERT INTO %s %s SELECT %s FROM (%s)t ON DUPLICATE KEY UPDATE %s", $this->to, "($this->field)", $this->field, strtr($this->from, [':fields' => $this->field]), implode(',', $updates));
        }

        try {
            getDI('db')->get($this->db)->createCommand($sql)->execute();
        } catch (Throwable $e) {
            App::error($e->getMessage());
        } finally {
            $msg->data = 1;
            $this->sink($msg);
        }
    }
}
