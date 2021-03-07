<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Data\Pipeline\Message;

class SynToMysql extends BaseSyncData
{
    public function sync(Message $msg): void
    {
        $updates = [];
        foreach (explode(',', $this->field) as $key) {
            $key = trim($key);
            $updates[] = "$key=values($key)";
        }
        $sql = sprintf("INSERT INTO %s %s %s ON DUPLICATE KEY UPDATE %s", $this->to, "($this->field)", strtr($this->from, [':fields' => $this->field]), implode(',', $updates));
        $msg->data = getDI('db')->get($this->db)->createCommand($sql)->execute();
        $this->sink($msg);
    }
}
