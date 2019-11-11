<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\helper\ArrayHelper;

class RemoveFields extends AbstractPlugin
{
    /** @var array */
    protected $fields = [];

    /**
     * @throws \Exception
     */
    public function run(): void
    {
        $params = ArrayHelper::toArray($this->input);
        foreach ($this->fields as $key) {
            unset($params[$key]);
        }
        $this->output($params);
    }
}
