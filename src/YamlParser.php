<?php

declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\FileHelper;

/**
 * Class YamlParser
 * @package Rabbit\Data\Pipeline
 */
class YamlParser implements ConfigParserInterface
{
    protected array $exts = ['yaml', 'yml'];

    public function __construct(public readonly string $path)
    {
        if (!is_dir($path) && !file_exists($path)) {
            throw new InvalidConfigException("The path must be dir or file");
        }
    }

    /**
     * @return array
     * @throws InvalidConfigException
     */
    public function parse(): array
    {
        $config = [];
        if (is_dir($this->path)) {
            FileHelper::dealFiles($this->path, [
                'filter' => function (string $path) use (&$config): bool {
                    if (!is_file($path)) {
                        return true;
                    }
                    if (!in_array(pathinfo($path, PATHINFO_EXTENSION), $this->exts)) {
                        return false;
                    }
                    $yaml = yaml_parse_file($path);
                    if ($yaml === false) {
                        throw new InvalidConfigException(error_get_last()['message'] . " path=$path");
                    }
                    $config[pathinfo($path, PATHINFO_FILENAME)] = $yaml;
                    return true;
                }
            ]);
        } else {
            $item = yaml_parse_file($this->path);
            if ($item === false) {
                throw new InvalidConfigException(error_get_last()['message'] . " path=$this->path");
            }
            $config[pathinfo($this->path, PATHINFO_FILENAME)] = $item;
        }
        return $config;
    }

    public function parseTask(string $key): array
    {
        $item = yaml_parse_file("{$this->path}/$key.yaml");
        if ($item === false) {
            throw new InvalidConfigException(error_get_last()['message'] . " key=$key");
        }
        return $item;
    }
}
