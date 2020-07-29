<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use DOMException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\XmlHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Throwable;

/**
 * Class XmlFormat
 * @package Rabbit\Data\Pipeline\Transforms
 */
class XmlFormat extends AbstractPlugin
{
    /** @var string */
    protected string $rootTag;
    /** @var string */
    protected string $itemTag;
    protected string $version;
    protected string $charset;

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $this->rootTag,
            $this->itemTag,
            $this->version,
            $this->charset
        ] = ArrayHelper::getValueByArray($this->config, [
            'rootTag',
            'itemTag',
            'version',
            'charset',
        ], [
            'root',
            'item',
            '1.0',
            'utf-8'
        ]);
    }

    /**
     * @param Message $msg
     * @throws DOMException
     */
    public function run(Message $msg): void
    {
        $msg->data = XmlHelper::format(
            $msg->data,
            $this->rootTag,
            $this->itemTag,
            $this->version,
            $this->charset
        );
        $this->sink($msg);
    }
}
