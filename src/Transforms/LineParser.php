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
    protected int $fieldLine = 0;
    /** @var string */
    protected ?string $fileType;
    /** @var string */
    protected string $delimiter = ',';
    /** @var string */
    protected string $enclosure = '"';
    /** @var string */
    protected string $escape = '\\';
    /** @var string */
    protected string $idKey = 'id';

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
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
            FileHelper::fgetsExt($msg->data, function ($fp) use ($comField, $msg) {
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
                            $columns = explode($this->explode, str_replace('-', '_', trim($line)));
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
                $msg->data = ['columns' => &$columns, 'data' => &$rows];
                $this->sink($msg);
            });
        } else {
            /** @var  $data */
            $data = explode($this->split, $msg->data);
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
            $msg->data = ['columns' => &$columns, 'data' => &$rows];
            $this->sink($msg);
        }
    }
}
