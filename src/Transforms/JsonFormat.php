<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use Exception;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\helper\JsonHelper;

/**
 * Class JsonFormat
 * @package Rabbit\Data\Pipeline\Transforms
 */
class JsonFormat extends AbstractPlugin
{
    /**
     * @throws Exception
     */
    public function run(): void
    {
        $output = JsonHelper::encode($this->input);
        $this->output($output);
    }
}
