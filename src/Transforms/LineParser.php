<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\FileHelper;

/**
 * Class LineParser
 * @package Rabbit\Data\Pipeline\Common
 */
class LineParser extends AbstractPlugin
{
    /** @var string */
    protected $split = PHP_EOL;
    /** @var string */
    protected $explode = '\t';
    /** @var int */
    protected $columnLine = 0;
    /** @var int */
    protected $dataLine = 1;
    /** @var array */
    protected $field = [];
    /** @var int */
    protected $fieldLine = 0;
    /** @var string */
    protected $fileType;
    /** @var string */
    protected $delimiter = ',';
    /** @var string */
    protected $enclosure = '"';
    /** @var string */
    protected $escape = '\\';

    /**
     * @return mixed|void
     */
    public function init()
    {
        parent::init();
        [
            $this->split,
            $this->columnLine,
            $this->dataLine,
            $this->field,
            $this->fieldLine,
            $this->fileType,
            $this->delimiter,
            $this->enclosure,
            $this->escape
        ] = ArrayHelper::getValueByArray(
            $this->config,
            [
                'split',
                'columnLine',
                'dataLine',
                'field',
                'fieldLine',
                'fileType',
                'delimiter',
                'enclosure',
                'escape'
            ],
            null,
            [
                PHP_EOL,
                0,
                1,
                [],
                null,
                null,
                ',',
                '"',
                '\\'
            ]
        );
        if (!$this->fileType) {
            throw new InvalidConfigException("fileType must be set in $this->key");
        }
        $this->fileType = strtolower($this->fileType);
    }

    /**
     * @throws \Throwable
     */
    public function run()
    {
        if (is_file($this->input)) {
            FileHelper::fgetsExt($this->input, function ($fp) {
                $i = 0;
                $field = [];
                $columns = [];
                $rows = [];
                while (!feof($fp)) {
                    if ($this->fileType === 'txt') {
                        $line = fgets($fp);
                    } elseif ($this->fileType === 'csv') {
                        $line = fgetcsv($fp, $this->delimiter, $this->enclosure, $this->escape);
                    }
                    if ($line) {
                        $i++;
                        $data = explode($this->explode, $line);
                        if ($i === $this->columnLine) {
                            $columns = $data;
                        } elseif ($this->fieldLine && $this->field && $i === $this->fieldLine) {
                            foreach ($this->field as $key => $index) {
                                $field[array_search($key, $columns)] = $value[$index];
                            }
                        } else {
                            foreach ($field as $index => $value) {
                                $data = array_splice($data, $index, 1, $value);
                            }
                            $rows[] = $data;
                        }
                    }
                }
                $output = ['columns' => &$columns, 'data' => &$rows];
                $this->output($output);
            });
        } else {
            /** @var  $data */
            $data = explode($this->split, $this->input);
            $columns = ArrayHelper::remove($data, $this->columnLine);
            $rows = [];
            foreach ($data as &$item) {
                if ($this->field && $this->fieldLine) {
                    $fields = ArrayHelper::remove($data, $this->fieldLine);
                    foreach ($this->field as $key => $index) {
                        $item = array_splice($item[array_search($key, $columns)], $fields[$index]);
                    }
                }
                $rows[] = $data;
            }
            $output = ['columns' => &$columns, 'data' => &$rows];
            $this->output($output);
        }
    }
}
