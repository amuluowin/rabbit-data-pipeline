<?php


namespace Rabbit\Data\Pipeline\Transforms;

use common\Helpers\XmlFormatHelper;
use Exception;
use rabbit\helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class XmlParser
 * @package Rabbit\Data\Pipeline\Transforms
 */
class XmlParser extends AbstractPlugin
{
    /** @var string */
    protected $rootTag;
    /** @var string */
    protected $objectTag;
    /** @var string */
    protected $itemTag;
    protected $version;
    protected $charset;

    /**
     * @return mixed|void
     */
    public function init()
    {
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
        ], null, [
            'root',
            '',
            'item',
            '1.0',
            'utf-8'
        ]);
    }

    /**
     * @param null $input
     * @throws Exception
     */
    public function input(&$input = null): void
    {
        $data = XmlFormatHelper::format(
            $input,
            $this->rootTag,
            $this->objectTag,
            $this->itemTag,
            $this->version,
            $this->charset
        );
        $this->output($data);
    }
}
