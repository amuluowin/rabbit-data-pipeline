<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\FileHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Throwable;
use Vtiful\Kernel\Excel;

/**
 * Class LineParser
 * @package Rabbit\Data\Pipeline\Common
 */
class LineParser extends AbstractPlugin
{
    protected string $split = PHP_EOL;
    protected string $explode = "\t";
    protected int $columnLine = 0;
    protected array $columns = [];
    protected array $dataLine = [1];
    protected ?int $endLine = null;
    protected array $field = [];
    protected ?int $fieldLine = 0;
    protected ?string $fileType;
    protected string $delimiter = ',';
    protected string $enclosure = '"';
    protected string $escape = '\\';
    protected ?string $idKey = null;
    protected array $include = [];
    protected array $exclude = [];
    protected ?string $sheet = null;
    protected array $map = [];
    protected array $deal = [];

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
            $this->columns,
            $this->dataLine,
            $this->field,
            $this->fieldLine,
            $this->fileType,
            $this->delimiter,
            $this->enclosure,
            $this->escape,
            $this->idKey,
            $this->sheet,
            $this->include,
            $this->exclude,
            $this->map,
            $this->deal,
        ] = ArrayHelper::getValueByArray(
            $this->config,
            [
                'split',
                'explode',
                'columnLine',
                'columns',
                'dataLine',
                'field',
                'fieldLine',
                'fileType',
                'delimiter',
                'enclosure',
                'escape',
                'idKey',
                'sheet',
                'include',
                'exclude',
                'map',
                'deal'
            ],
            [
                PHP_EOL,
                "\t",
                0,
                [],
                [1],
                [],
                null,
                null,
                ',',
                '"',
                '\\',
                null,
                null,
                [],
                [],
                [],
                []
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
        $comField = $field = $columns = $rows = [];
        if (isset($msg->opt['comField']) && is_array($msg->opt['comField'])) {
            $comField = $msg->opt['comField'];
        }
        $i = 1;
        if (is_file($msg->data)) {
            if (in_array($this->fileType, ['xls', 'xlsx'])) {
                $config = ['path' => dirname($msg->data)];
                $excel = new Excel($config);
                $excel->openFile(basename($msg->data))->openSheet($this->sheet, Excel::SKIP_EMPTY_ROW);
                while ($line = $excel->nextRow()) {
                    if ($this->endLine && $i >= $this->endLine) {
                        break;
                    }
                    if ($i === $this->columnLine || ($this->field && $i === $this->fieldLine) || in_array($i, $this->dataLine) || $i >= max($this->dataLine)) {
                        $this->makeData($i, $field, $comField, $line, $columns, $rows);
                    }
                    $i++;
                }
            } else {
                FileHelper::fgetsExt($msg->data, function ($fp) use ($comField, &$i, &$field, &$columns, &$rows, $msg) {
                    while (!feof($fp)) {
                        if ($this->endLine && $i >= $this->endLine) {
                            break;
                        }
                        if ($this->fileType === 'txt') {
                            $line = fgets($fp);
                            $line = explode($this->explode, trim($line));
                        } else {
                            $line = fgetcsv($fp, $this->delimiter, $this->enclosure, $this->escape);
                        }
                        if ($line && ($i === $this->columnLine || ($this->field && $i === $this->fieldLine) || in_array($i, $this->dataLine) || $i >= max($this->dataLine))) {
                            $this->makeData($i, $field, $comField, $line, $columns, $rows);
                        }
                        $i++;
                    }
                });
            }
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
                if ($this->endLine && $i >= $this->endLine) {
                    break;
                }
                if ($i === $this->columnLine || ($this->field && $i === $this->fieldLine) || in_array($i, $this->dataLine) || $i >= max($this->dataLine)) {
                    $item = explode($this->explode, $item);
                    foreach ($this->exclude as $index) {
                        unset($item[$index]);
                    }
                    $this->dealInclude($item);
                    if (isset($field)) {
                        array_splice($item, 0, 0, array_values($field));
                    }
                }
                $i++;
            }
        }
        if (empty($columns)) {
            $columns = $this->columns;
        }
        $columns = array_merge($columns, array_keys($comField));
        $this->idKey && ($columns[] = $this->idKey);
        foreach ($this->map as $col => $new) {
            $index = array_search($col, $columns, true);
            $columns[$index] = $new;
        }
        $msg->data = ['columns' => &$columns, 'data' => &$rows];
        $this->sink($msg);
    }

    /**
     * @param int $i
     * @param array $field
     * @param array $line
     * @param array $rows
     * @throws Throwable
     */
    private function makeData(int &$i, array &$field, array &$comField, array &$line, array &$columns, array &$rows): void
    {
        foreach ($this->exclude as $index) {
            unset($line[$index]);
        }
        if ($i === $this->columnLine) {
            if ($this->include) {
                $columns = ArrayHelper::getValueByArray($line, array_keys($this->include));
            }
        } elseif ($this->fieldLine && $this->field && $i === $this->fieldLine) {
            foreach ($this->field as $key => $index) {
                $field[array_search($key, $columns, true)] = $line[$index];
            }
            array_splice($columns, 0, 0, array_keys($field));
        } else {
            $this->dealInclude($line);
            array_splice($line, 0, 0, array_values($field));
            $line = array_merge($line, array_values($comField));
            $this->idKey && ($line[] = getDI('idGen')->create());
            $rows[] = $line;
        }
    }

    /**
     * @param array $line
     */
    private function dealInclude(array &$line): void
    {
        if ($this->include) {
            $tmpCols = [];
            foreach ($this->include as $index => $deal) {
                $deal && ($line[$index] = eval('$col=$line[$index];' . $deal));
            }
            $remove = array_diff(array_keys($line), array_keys($this->include));
            foreach ($remove as $index) {
                unset($line[$index]);
            }
        }
    }
}
