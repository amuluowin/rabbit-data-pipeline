<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use rabbit\App;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\helper\ArrayHelper;

/**
 * Class XmlParser
 * @package Rabbit\Data\Pipeline\Transforms
 */
class XmlParser extends AbstractPlugin
{
    /** @var array */
    protected $fields = [];

    public function init()
    {
        parent::init();
        $this->fields = ArrayHelper::getValue($this->config, 'fields', []);
    }

    /**
     * @throws \Exception
     */
    public function run(): void
    {
        if (!is_string($this->input)) {
            App::warning("$this->taskName $this->key must input path or xml string");
        }
        if (is_file($this->input) && file_exists($this->input)) {
            $xml = json_decode(json_encode(simplexml_load_file($this->input), JSON_UNESCAPED_UNICODE), true);
        } else {
            $xml = json_decode(json_encode(simplexml_load_string($this->input), JSON_UNESCAPED_UNICODE), true);
        }
        $params = [];
        foreach ($this->fields as $field => $item) {
            if (is_array($item)) {
                foreach ($item as $key) {
                    $params[$field] = ArrayHelper::getValue($xml, $key, ArrayHelper::getValue($params, $field));
                }
            } else {
                $params[$field] = ArrayHelper::getValue($xml, $item);
            }
        }
        empty($params) && ($params = &$xml);
        $this->output($params);
    }
}
