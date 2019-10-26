<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use rabbit\App;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class XmlParser
 * @package Rabbit\Data\Pipeline\Transforms
 */
class XmlParser extends AbstractPlugin
{
    /**
     * @param null $input
     */
    public function input(&$input = null): void
    {
        if (!is_string($input)) {
            App::warning("$this->taskName $this->key must input path or xml string");
        }
        if (is_file($input) && file_exists($input)) {
            $this->output(simplexml_load_file($input));
        } else {
            $this->output(simplexml_load_string($input));
        }
    }
}
