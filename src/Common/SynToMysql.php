<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\App;
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
        $sql = sprintf("INSERT INTO %s %s select %s from (%s)t ON DUPLICATE KEY UPDATE %s", $this->to, "($this->field)", $this->field, strtr($this->from, [':fields' => $this->field]), implode(',', $updates));
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
