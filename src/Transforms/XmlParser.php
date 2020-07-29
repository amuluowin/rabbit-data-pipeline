<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use Rabbit\Base\App;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Throwable;

/**
 * Class XmlParser
 * @package Rabbit\Data\Pipeline\Transforms
 */
class XmlParser extends AbstractPlugin
{
    /** @var array */
    protected array $fields = [];

    public function init(): void
    {
        parent::init();
        $this->fields = (array)ArrayHelper::getValue($this->config, 'fields', []);
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        if (!is_string($msg->data)) {
            App::warning("$this->taskName $this->key must input path or xml string");
        }
        if (is_file($msg->data) && file_exists($msg->data)) {
            $xml = json_decode(json_encode(simplexml_load_file($msg->data), JSON_UNESCAPED_UNICODE), true);
        } else {
            $xml = json_decode(json_encode(simplexml_load_string($msg->data), JSON_UNESCAPED_UNICODE), true);
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
        $msg->data = $params;
        $this->sink($msg);
    }
}
