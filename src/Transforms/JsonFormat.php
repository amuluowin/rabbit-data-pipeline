<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use Exception;
use rabbit\helper\JsonHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class JsonFormat
 * @package Rabbit\Data\Pipeline\Transforms
 */
class JsonFormat extends AbstractPlugin
{
    /**
     * @param null $input
     * @param array $opt
     * @throws Exception
     */
    public function input(&$input = null, &$opt = []): void
    {
        $this->output(JsonHelper::encode($input));
    }
}
