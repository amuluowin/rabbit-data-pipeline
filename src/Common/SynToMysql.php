<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\App;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\Message;
use Throwable;

class SynToMysql extends BaseSyncData
{
    protected string $mode;
    protected string $where;


    public function init(): void
    {
        parent::init();
        [
            $this->mode,
            $this->where,
        ] = ArrayHelper::getValueByArray($this->config, ['mode', 'where'], ['INSERT', '']);
        $this->mode = strtoupper($this->mode);
    }

    public function run(Message $msg): void
    {
        $fields = '';
        $updates = [];
        $primary = empty($this->primary) ? $this->primary : explode(',',  $this->primary);
        foreach (explode(',', $this->field) as $key) {
            $key = trim($key);
            $fields .= "f.$key,";
            if (!empty($primary) && in_array($key, $primary)) {
                continue;
            }
            $updates[] = "$key=values($key)";
        }
        $fields = rtrim($fields, ',');

        if ($this->equal) {
            $equal = '';
            foreach (explode(',', $this->equal) as $key) {
                $equal .= "f.$key=t.$key and ";
            }
            $equal = rtrim($equal, ' and ');
            $sql = "{$this->mode} INTO {$this->to} ({$this->field}) SELECT {$fields} FROM {$this->from} f WHERE NOT EXISTS (SELECT 1 FROM {$this->to} t WHERE $equal)" . ($this->where ? "and {$this->where}" : '');
        } else {
            $sql = "{$this->mode} INTO {$this->to} ({$this->field}) SELECT {$fields} FROM ({$this->from}) f" . ($this->where ? " where {$this->where}" : '');
        }

        if ($this->mode === 'INSERT' && !$this->onlyInsert) {
            $sql .= " ON DUPLICATE KEY UPDATE " . implode(',', $updates);
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
