<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\ActiveRecord\ARHelper;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\Expression;
use Rabbit\DB\Query;

class SyncOtherMysql extends AbstractPlugin
{
    protected int $size = 3000;
    protected int $sleep = 3600;
    protected array $from = [];
    protected array $to = [];
    protected array $incr = [];
    protected array $replace = [];
    public function init(): void
    {
        parent::init();
        [
            $this->size,
            $this->sleep,
            $this->from,
            $this->to,
            $this->replace,
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['size', 'sleep', 'from', 'to', 'replace'],
            [$this->size, $this->sleep, $this->from, $this->to, $this->replace]
        );
        if (empty($this->from) || empty($this->to)) {
            throw new InvalidArgumentException("from or to is empty");
        }
    }

    public function run(Message $msg): void
    {
        loop(function () use ($msg) {
            $query = (new Query(getDI('db')->get($this->from['db'])))->from([$this->from['table']]);
            if ($this->size > 0) {
                $query->limit($this->size);
            }
            if ($this->from['max'] ?? false && $this->to['max'] ?? false) {
                $query->filterWhere(['>', $this->from['max'], (new Query(getDI('db')->get($this->to['db'])))->select([new Expression("max({$this->to['max']})")])->from([$this->to['table']])->scalar()]);
                while ($data = $query->all()) {
                    $this->sync($data);
                }
            } else {
                $data = $query->all();
                $this->sync($data);
            }
        }, $this->sleep * 1000);
    }

    private function sync(array &$data): void
    {
        if ($this->replace) {
            foreach ($data as &$item) {
                foreach ($this->replace as $key => $value) {
                    $item[$value] = $item[$key];
                    unset($item[$key]);
                }
            }
        }
        ARHelper::update(ARHelper::getModel($this->to['table'], $this->to['db']), $data);
    }
}
