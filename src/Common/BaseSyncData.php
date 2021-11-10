<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;

abstract class BaseSyncData extends AbstractPlugin
{
    protected ?string $equal;
    protected string $from;
    protected string $to;
    protected string $field;
    protected string $db;
    protected ?string $primary;
    protected bool $onlyInsert;
    protected ?int $batch;
    protected ?string $sql = null;

    public function init(): void
    {
        [
            $this->from,
            $this->to,
            $this->db,
            $this->field,
            $this->equal,
            $this->primary,
            $this->onlyInsert,
            $this->batch,
            $this->sql,
        ] = ArrayHelper::getValueByArray($this->config, ['from', 'to', 'db', 'field', 'equal', 'primary', 'onlyInsert', 'batch', 'sql'], ['db' => 'default', 'onlyInsert' => false]);
        if ($this->from === null || $this->to === null || $this->field === null) {
            throw new InvalidConfigException('from or to or field is empty!');
        }
    }
}
