<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;

abstract class BaseSyncData extends AbstractPlugin
{
    protected string $from;
    protected string $to;
    protected string $field;
    protected string $db;

    public function init(): void
    {
        [
            $this->from,
            $this->to,
            $this->db,
            $this->field
        ] = ArrayHelper::getValueByArray($this->config, ['from', 'to', 'db', 'field'], ['db' => 'default']);
        if ($this->from === null || $this->to === null || $this->field === null) {
            throw new InvalidConfigException('from or to or field is empty!');
        }
    }
}
