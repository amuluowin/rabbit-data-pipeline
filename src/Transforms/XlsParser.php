<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;


use DI\DependencyException;
use DI\NotFoundException;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use ReflectionException;
use Throwable;
use Vtiful\Kernel\Excel;

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
     * @param Message $msg
     * @throws Throwable
     * @throws DependencyException
     * @throws NotFoundException
     * @throws InvalidConfigException
     * @throws ReflectionException
     */
    public function run(Message $msg): void
    {
        if (!is_file($msg->data)) {
            throw new InvalidArgumentException("input is not a file!");
        }
        $excel = new Excel(['path' => dirname($msg->data)]);
        $excel->openFile(pathinfo($msg->data, PATHINFO_FILENAME))->openSheet();
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
        $msg->data = ['columns' => &$columns, 'data' => &$rows];
        $this->sink($msg);
    }
}