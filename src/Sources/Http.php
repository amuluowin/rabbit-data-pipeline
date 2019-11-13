<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use Exception;
use rabbit\helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Swlib\SaberGM;

/**
 * Class Http
 * @package Rabbit\Data\Pipeline\Sources
 */
class Http extends AbstractPlugin
{

    /**
     * @throws Exception
     */
    public function run(): void
    {
        $format = ArrayHelper::remove($this->input, 'format');
        $response = SaberGM::request(array_merge($this->config, ArrayHelper::toArray($this->input)));
        $parseType = "getParsed$format";
        if (method_exists($response, $parseType)) {
            $result = $response->$parseType();
        } else {
            $result = htmlspecialchars((string)$response->getBody());
        }
        $this->output($result);
    }
}
