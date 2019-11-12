<?php


namespace Rabbit\Data\Pipeline\Common;


use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\helper\ArrayHelper;
use rabbit\helper\FileHelper;

/**
 * Class TxtParser
 * @package Rabbit\Data\Pipeline\Transforms
 */
class TxtParser extends AbstractPlugin
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
    protected $type = 'clickhouse';
    /** @var string */
    protected $dsn;

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
        ] = ArrayHelper::getValueByArray($this->config, ['split', 'columnLine', 'dataLine', 'fieldLine'], null, [
            PHP_EOL,
            0,
            1,
            [],
            null
        ]);
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
                while (!feof($fp)) {
                    $line = fgets($fp);
                    $i++;
                    $data = explode($this->explode, $line);
                    if ($i === $this->columnLine) {
                        $columns = ['columns' => $data];
                        $this->output($columns);
                    } elseif ($this->fieldLine && $this->field && $i === $this->fieldLine) {
                        foreach ($this->field as $key => $index) {
                            $field[array_search($key, $columns)] = $value[$index];
                        }
                    } else {
                        foreach ($field as $index => $value) {
                            $data = array_splice($data, $index, 1, $value);
                        }
                        $this->output($data);
                    }
                }
            });
        } else {
            /** @var  $data */
            $data = explode($this->split, $this->input);
            $columns = ArrayHelper::remove($data, $this->columnLine);
            if ($this->field && $this->fieldLine) {
                $fields = ArrayHelper::remove($data, $this->fieldLine);
                foreach ($this->field as $key => $index) {
                    foreach ($data as &$item) {
                        $item = array_splice($item[array_search($key, $columns)], $fields[$index]);
                    }
                }
            }
            $output = ['columns' => $columns, 'data' => $data];
            $this->output($output);
        }
    }
}