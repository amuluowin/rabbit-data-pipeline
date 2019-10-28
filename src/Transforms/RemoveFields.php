<?php


namespace Rabbit\Data\Pipeline\Transforms;

use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\helper\ArrayHelper;

class RemoveFields extends AbstractPlugin
{
    /** @var array */
    protected $fields = [];

    /**
     * @param null $input
     */
    public function input(&$input = null)
    {
        $params = $input;
        $params = ArrayHelper::toArray($params);
        foreach ($this->fields as $key) {
            unset($params[$key]);
        }
        $this->output($params);
    }
}
