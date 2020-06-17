<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Transforms;

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
    protected $explode = "\t";
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
    /** @var string */
    protected $idKey = 'id';

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
            $this->escape,
            $this->idKey
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
                'escape',
                'idKey'
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
                '\\',
                'id'
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
        $comField = [];
        if (isset($this->opt['comField']) && is_array($this->opt['comField'])) {
            $comField = $this->opt['comField'];
        }
        if (is_file($this->input)) {
            FileHelper::fgetsExt($this->input, function ($fp) use ($comField) {
                $i = 0;
                $columns = $rows = $field = [];
                while (!feof($fp)) {
                    if ($this->fileType === 'txt') {
                        $line = fgets($fp);
                    } elseif ($this->fileType === 'csv') {
                        $line = fgetcsv($fp, $this->delimiter, $this->enclosure, $this->escape);
                    }
                    if ($line) {
                        $i++;
                        $data = explode($this->explode, $line);
                        $data[] = trim(array_pop($data));
                        if ($i === $this->columnLine) {
                            $columns = explode($this->explode, str_replace('-','_', trim($line)));
                        } elseif ($this->fieldLine && $this->field && $i === $this->fieldLine) {
                            foreach ($this->field as $key => $index) {
                                $field[array_search($key, $columns)] = $data[$index];
                            }
                            array_splice($columns, 0, 0, array_keys($field));
                        } else {
                            array_splice($data, 0, 0, array_values($field));
                            $data = array_merge($data, array_values($comField));
                            $data[] = getDI('idGen')->create();
                            $rows[] = $data;
                        }
                    }
                }
                $columns = array_merge($columns, array_keys($comField));
                $columns[] = $this->idKey;
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
