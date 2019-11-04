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
     * @throws \Exception
     */
    public function run(): void
    {
        $output = ArrayHelper::toArray($this->input);
        $this->output($output);
    }
}
