<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;


use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Throwable;

/**
 * Class XlsParser
 * @package Rabbit\Data\Pipeline\Transforms
 */
class XlsParser extends AbstractPlugin
{
    /** @var int */
    protected int $columnLine = 0;
    /** @var int */
    protected int $dataLine = 1;

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
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
            [
                0,
                1
            ]
        );
    }

    /**
     * @throws Throwable
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
        $columns = [];
        $rows = [];
        while ($row = $excel->nextRow()) {
            if ($i === $this->columnLine) {
                $columns = $row;
            } elseif ($i >= $this->dataLine) {
                $rows[] = $row;
            }
        }
        $output = ['columns' => &$columns, 'data' => &$rows];
        $this->output($output);
    }
}