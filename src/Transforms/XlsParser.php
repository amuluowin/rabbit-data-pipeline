<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;


use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\exception\InvalidArgumentException;
use rabbit\helper\ArrayHelper;

/**
 * Class XlsParser
 * @package Rabbit\Data\Pipeline\Transforms
 */
class XlsParser extends AbstractPlugin
{
    /** @var int */
    protected $columnLine = 0;
    /** @var int */
    protected $dataLine = 1;

    /**
     * @return mixed|void
     */
    public function init()
    {
        parent::init();
        [
            $this->columnLine,
            $this->dataLine
        ] = ArrayHelper::getValueByArray(
            $this->config,
            [
                'columnLine',
                'dataLine'
            ],
            null,
            [
                0,
                1
            ]
        );
    }

    /**
     * @throws \Exception
     */
    public function run()
    {
        if (!is_file($this->input)) {
            throw new InvalidArgumentException("input is not a file!");
        }
        $excel = new \Vtiful\Kernel\Excel(['path' => dirname($this->input)]);
        $excel->openFile(pathinfo($this->input, PATHINFO_FILENAME))
            ->openSheet();
        $i = 0;
        while ($row = $excel->nextRow()) {
            if ($i === $this->columnLine) {
                $this->output(['columns' => $row]);
            } elseif ($i >= $this->dataLine) {
                $this->output($row);
            }
        }
    }
}