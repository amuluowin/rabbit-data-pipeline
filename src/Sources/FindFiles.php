<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sources;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\FileHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Throwable;

/**
 * Class FindFiles
 * @package Rabbit\Data\Pipeline\Sources
 */
class FindFiles extends AbstractPlugin
{
    protected ?string $fileName = null;

    protected ?string $scanDir = null;

    protected ?array $extends = [];

    public function init(): void
    {
        parent::init();
        [
            $this->fileName,
            $this->scanDir,
            $this->extends,
        ] = ArrayHelper::getValueByArray($this->config, ['fileName', 'scanDir', 'extends'], ['extends' => []]);

        if (!$this->fileName && !$this->scanDir) {
            throw new InvalidConfigException("fileName or scanDir has one and only one");
        }
        if ($this->fileName && !is_file($this->fileName)) {
            throw new InvalidConfigException("fileName must be a file");
        }
        if ($this->scanDir && !is_dir($this->scanDir)) {
            throw new InvalidConfigException("scanDir must be a dir");
        }
        if ($this->scanDir && empty($this->extends)) {
            throw new InvalidConfigException("if set scanDir you must set extends too");
        }
    }

    /**
     * @param Message $msg
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        if ($this->fileName) {
            $msg->data = $this->fileName;
            $this->sink($msg);
        } else {
            FileHelper::dealFiles($this->scanDir, [
                'filter' => function (string $path) use ($msg): bool {
                    if (!is_file($path)) {
                        return true;
                    }
                    if (!in_array(pathinfo($path, PATHINFO_EXTENSION), $this->extends)) {
                        return false;
                    }
                    $tmp = clone $msg;
                    $tmp->data = $path;
                    $this->sink($tmp);
                    return true;
                }
            ]);
        }
    }
}
