<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\FileHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Throwable;

/**
 * Class LineParser
 * @package Rabbit\Data\Pipeline\Common
 */
class LineParser extends AbstractPlugin
{
    /** @var string */
    protected string $split = PHP_EOL;
    /** @var string */
    protected string $explode = "\t";
    /** @var int */
    protected int $columnLine = 0;
    /** @var int */
    protected int $dataLine = 1;
    /** @var array */
    protected array $field = [];
    /** @var int */
    protected ?int $fieldLine = 0;
    /** @var string */
    protected ?string $fileType;
    /** @var string */
    protected string $delimiter = ',';
    /** @var string */
    protected string $enclosure = '"';
    /** @var string */
    protected string $escape = '\\';
    /** @var string */
    protected ?string $idKey = null;
    /** @var array */
    protected array $cols = [];
    /** @var string|null */
    protected ?string $sheet = null;

    const SUPPORT_EXT = [
        'xls',
        'xlsx',
        'txt',
        'csv'
    ];

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [
            $this->split,
            $this->explode,
            $this->columnLine,
            $this->dataLine,
            $this->field,
            $this->fieldLine,
            $this->fileType,
            $this->delimiter,
            $this->enclosure,
            $this->escape,
            $this->idKey,
            $this->sheet
        ] = ArrayHelper::getValueByArray(
            $this->config,
            [
                'split',
                'explode',
                'columnLine',
                'dataLine',
                'field',
                'fieldLine',
                'fileType',
                'delimiter',
                'enclosure',
                'escape',
                'idKey',
                'sheet'
            ],
            [
                PHP_EOL,
                "\t",
                0,
                1,
                [],
                null,
                null,
                ',',
                '"',
                '\\',
                null,
                null
            ]
        );
        if (!$this->fileType || !in_array($this->fileType, self::SUPPORT_EXT)) {
            throw new InvalidConfigException(sprintf("fileType only support (%s)", implode(' & ', self::SUPPORT_EXT)));
        }
        if (in_array($this->fileType, ['xls', 'xlsx']) && !$this->sheet) {
            throw new InvalidConfigException("When xls or xlsx you must set sheet");
        }
        if (in_array($this->fileType, ['xls', 'xlsx']) && !extension_loaded('xlswriter')) {
            throw new InvalidConfigException("When xls or xlsx you must use ext-xlswriter");
        }
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        $comField = [];
        if (isset($msg->opt['comField']) && is_array($msg->opt['comField'])) {
            $comField = $msg->opt['comField'];
        }
        if (is_file($msg->data)) {
            $field = $columns = $rows = [];
            $i = 0;
            if (in_array($this->fileType, ['xls', 'xlsx'])) {
                $config = ['path' => dirname($msg->data)];
                $excel = new \Vtiful\Kernel\Excel($config);
                $excel->openFile(basename($msg->data))->openSheet($this->sheet, Excel::SKIP_EMPTY_ROW);
                while ($line = $excel->nextRow()) {
                    $this->makeData($i, $field, $line, $columns, $rows);
                }
            } else {
                FileHelper::fgetsExt($msg->data, function ($fp) use ($comField, &$i, &$field, &$columns, &$rows, $msg) {
                    while (!feof($fp)) {
                        if ($this->fileType === 'txt') {
                            $line = fgets($fp);
                            $line = explode($this->explode, trim($line));
                        } else {
                            $line = fgetcsv($fp, $this->delimiter, $this->enclosure, $this->escape);
                        }
                        if ($line) {
                            $this->makeData($i, $field, $line, $columns, $rows);
                        }
                    }
                });
            }
            $columns = array_merge($columns, array_keys($comField));
            $this->idKey && ($columns[] = $this->idKey);
            $msg->data = ['columns' => &$columns, 'data' => &$rows];
            $this->sink($msg);
        } else {
            $rows = explode($this->split, $msg->data);
            $columns = explode($this->explode, ArrayHelper::remove($data, $this->columnLine, []));
            if ($this->field && $this->fieldLine) {
                $line = explode($this->explode, ArrayHelper::remove($data, $this->fieldLine));
                foreach ($this->field as $key => $index) {
                    $field[array_search($key, $columns, true)] = $line[$index];
                }
            }
            foreach ($rows as &$item) {
                $item = explode($this->explode, $item);
                if (isset($field)) {
                    array_splice($item, 0, 0, array_values($field));
                }
            }
            $msg->data = ['columns' => &$columns, 'data' => &$rows];
            $this->sink($msg);
        }
    }

    /**
     * @param int $i
     * @param array $field
     * @param array $line
     * @param array $rows
     * @throws Throwable
     */
    private function makeData(int &$i, array &$field, array &$line, array &$columns, array &$rows): void
    {
        $i++;
        if ($i === $this->columnLine) {
            if ($this->cols) {
                $columns = ArrayHelper::getValueByArray($columns, $this->cols);
            }
        } elseif ($this->fieldLine && $this->field && $i === $this->fieldLine) {
            foreach ($this->field as $key => $index) {
                $field[array_search($key, $columns, true)] = $line[$index];
            }
            array_splice($columns, 0, 0, array_keys($field));
        } else {
            array_splice($data, 0, 0, array_values($field));
            $line = array_merge($data, array_values($comField));
            $this->idKey && ($data[] = getDI('idGen')->create());
            $rows[] = $line;
        }
    }
}
