<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline;

use rabbit\exception\InvalidConfigException;
use rabbit\helper\FileHelper;

/**
 * Class YamlParser
 * @package Rabbit\Data\Pipeline
 */
class YamlParser implements ConfigParserInterface
{
    /** @var string */
    protected $path;

    /**
     * YamlParser constructor.
     * @param string $path
     * @throws InvalidConfigException
     */
    public function __construct(string $path)
    {
        if (!is_dir($path) && !file_exists($path)) {
            throw new InvalidConfigException("The path must be dir or file");
        }
        $this->path = $path;
    }

    /**
     * @return string
     */
    public function getPath(): string
    {
        return $this->path;
    }

    /**
     * @return array
     * @throws InvalidConfigException
     */
    public function parse(): array
    {
        $config = [];
        if (is_dir($this->path)) {
            FileHelper::findFiles($this->path, [
                'filter' => function ($path) use (&$config) {
                    if (!is_file($path)) {
                        return true;
                    }
                    if (pathinfo($path, PATHINFO_EXTENSION) !== 'yaml') {
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
            $config = yaml_parse_file($this->path);
            if ($config === false) {
                throw new InvalidConfigException(error_get_last()['message'] . " path=$this->path");
            }
        }
        return $config;
    }
}
