<?php


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
     * @param $input
     * @throws Exception
     */
    public function input(&$input = null): void
    {
        $format = ArrayHelper::remove($input, 'format');
        $response = SaberGM::request(array_merge($this->config, ArrayHelper::toArray($input)));
        $parseType = "getParsed$format";
        if (method_exists($response, $parseType)) {
            $result = $response->$parseType();
        } else {
            $result = htmlspecialchars((string)$response->getBody());
        }
        $this->output($result);
    }
}
