<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use rabbit\helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class ArrayParser
 * @package Rabbit\Data\Pipeline\Transforms
 */
class ArrayParser extends AbstractPlugin
{
    /**
     * @param $input
     * @throws \Exception
     */
    public function input(&$input = null): void
    {
        $input = ArrayHelper::toArray($input);
        $this->output($input);
    }
}
