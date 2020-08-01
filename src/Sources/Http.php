<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Swlib\SaberGM;
use Throwable;

/**
 * Class Http
 * @package Rabbit\Data\Pipeline\Sources
 */
class Http extends AbstractPlugin
{
    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        $format = ArrayHelper::remove($this->config, 'format');
        $response = SaberGM::request($this->config);
        if (isset($this->config['download_dir'])) {
            $msg->data = $this->config['download_dir'];
        } else {
            $parseType = "getParsed$format";
            if (method_exists($response, $parseType)) {
                $msg->data = $response->$parseType();
            } else {
                $msg->data = htmlspecialchars((string)$response->getBody());
            }
        }
        $this->sink($msg);
    }
}
