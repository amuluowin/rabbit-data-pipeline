<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\ActiveRecord\ARHelper;
use Rabbit\Base\App;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Rabbit\DB\Expression;
use Rabbit\DB\Mysql\Connection;
use Rabbit\DB\Query;

class SyncOtherMysql extends AbstractPlugin
{
    protected int $size = 3000;
    protected int $sleep = 3600;
    protected array $from = [];
    protected array $to = [];
    protected array $incr = [];
    protected array $replace = [];
    protected array $exclude = [];
    protected int $parallel = 10;
    public function init(): void
    {
        parent::init();
        [
            $this->size,
            $this->sleep,
            $this->from,
            $this->to,
            $this->replace,
            $this->exclude,
        ] = ArrayHelper::getValueByArray(
            $this->config,
            ['size', 'sleep', 'from', 'to', 'replace', 'exclude'],
            [$this->size, $this->sleep, $this->from, $this->to, $this->replace, $this->exclude]
        );
        if (empty($this->from) || empty($this->to) || empty($this->parallel)) {
            throw new InvalidArgumentException("from or to or parallel is empty");
        }
    }

    public function run(Message $msg): void
    {
        loop(function (): void {
            $query = (new Query(service('db')->get($this->from['db'])))->from([$this->from['table']])->shareType(Connection::SHARE_ARRAY);
            if ($this->from['max'] ?? false && $this->to['max'] ?? false) {
                if ($this->size > 0) {
                    $query->limit($this->size * $this->parallel);
                }
                $query->filterWhere(['>', $this->from['max'], (new Query(service('db')->get($this->to['db'])))->select([new Expression("max({$this->to['max']})")])->from([$this->to['table']])->scalar()])
                    ->orderBy([$this->from['max'] => SORT_ASC]);
                $i = 0;
                while ($data = $query->offset($i * $this->size * $this->parallel)->all()) {
                    $this->sync($data);
                    $i++;
                }
            } else {
                $data = $query->all();
                $this->sync($data);
            }
            App::info("sync from {$this->from['db']}.{$this->from['table']} to {$this->to['db']}.{$this->to['table']}");
        }, $this->sleep * 1000);
    }

    private function sync(array &$data): void
    {
        if ($this->replace || $this->exclude) {
            foreach ($data as &$item) {
                foreach ($this->replace as $key => $value) {
                    $item[$value] = $item[$key];
                    unset($item[$key]);
                }
            }
        }
        if ($this->size > 0) {
            $data = array_chunk($data, $this->size);
            wgeach($data, fn (int $i, array $items): array => ARHelper::update(ARHelper::getModel($this->to['table'], $this->to['db']), $items, when: $this->exclude));
        } else {
            ARHelper::update(ARHelper::getModel($this->to['table'], $this->to['db']), $data, when: $this->exclude);
        }
    }
}
