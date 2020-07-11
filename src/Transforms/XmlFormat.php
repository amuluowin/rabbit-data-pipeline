<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use Exception;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class XmlFormat
 * @package Rabbit\Data\Pipeline\Transforms
 */
class XmlFormat extends AbstractPlugin
{
    /** @var string */
    protected string $rootTag;
    /** @var string */
    protected string $objectTag;
    /** @var string */
    protected string $itemTag;
    protected string $version;
    protected string $charset;

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init()
    {
        parent::init();
        [
            $this->rootTag,
            $this->objectTag,
            $this->itemTag,
            $this->version,
            $this->charset
        ] = ArrayHelper::getValueByArray($this->config, [
            'rootTag',
            'objectTag',
            'itemTag',
            'version',
            'charset',
        ], [
            'root',
            '',
            'item',
            '1.0',
            'utf-8'
        ]);
    }

    /**
     * @throws Exception
     */
    public function run(): void
    {
        $data = XmlFormatHelper::format(
            $this->input,
            $this->rootTag,
            $this->objectTag,
            $this->itemTag,
            $this->version,
            $this->charset
        );
        $this->output($data);
    }
}
