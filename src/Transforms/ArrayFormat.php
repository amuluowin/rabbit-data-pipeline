<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use rabbit\helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class ArrayFormat
 * @package Rabbit\Data\Pipeline\Transforms
 */
class ArrayFormat extends AbstractPlugin
{
    /**
     * @param null $input
     * @param array $opt
     * @throws \Exception
     */
    public function input(&$input = null, &$opt = []): void
    {
        $input = ArrayHelper::toArray($input);
        $this->output($input);
    }
}
