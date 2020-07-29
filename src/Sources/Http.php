<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use ReflectionException;
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
     * @throws DependencyException
     * @throws NotFoundException
     * @throws InvalidConfigException
     * @throws ReflectionException
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        $format = ArrayHelper::remove($msg->data, 'format');
        $response = SaberGM::request(array_merge($this->config, ArrayHelper::toArray($msg->data)));
        $parseType = "getParsed$format";
        if (method_exists($response, $parseType)) {
            $msg->data = $response->$parseType();
        } else {
            $msg->data = htmlspecialchars((string)$response->getBody());
        }
        $this->sink($msg);
    }
}
