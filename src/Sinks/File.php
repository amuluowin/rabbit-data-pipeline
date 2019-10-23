<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use common\Helpers\FileHelper;
use common\Helpers\XmlFormatHelper;
use rabbit\App;
use rabbit\core\Exception;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\ExceptionHelper;
use rabbit\helper\VarDumper;
use Rabbit\Data\Pipeline\AbstractPlugin;

/**
 * Class File
 * @package Rabbit\Data\Pipeline\Sinks
 */
class File extends AbstractPlugin
{
    /** @var string */
    protected $path;
    /** @var string */
    protected $fileName;

    /**
     * @return mixed|void
     * @throws InvalidConfigException
     */
    public function init()
    {
        [$configPath, $this->fileName] = ArrayHelper::getValueByArray($this->config, ['path', 'fileName']);
        if (empty($configPath)) {
            throw new InvalidConfigException("The path must be set in $this->key");
        }
        $this->path = App::getAlias($configPath);
    }


    /**
     * @param null $input
     * @throws \Exception
     */
    public function input(&$input = null): void
    {
        if ($this->fileName) {
            $this->saveFile($this->fileName, $input);
        } else {
            foreach ($input as $fileName => $data) {
                $this->saveFile(strtr($this->path, ['{fileName}' => $fileName]), $data);
            }
        }
    }

    /**
     * @param string $fileName
     * @param $data
     * @throws Exception
     * @throws \Exception
     */
    protected function saveFile(string $fileName, &$data): void
    {
        FileHelper::createDirectory(dirname($fileName), 777);
        $ext = strtolower(pathinfo($fileName, PATHINFO_EXTENSION));
        switch ($ext) {
            case 'csv':
                if (false === $fp = fopen($fileName, 'w+')) {
                    App::error("can not open file $fileName", $this->logKey);
                    return;
                }
                try {
                    foreach (ArrayHelper::toArray($data) as $item) {
                        fputcsv($fp, $item);
                    }
                } catch (\Throwable $throwable) {
                    App::error(ExceptionHelper::dumpExceptionToString($throwable), $this->logKey);
                } finally {
                    fclose($fp);
                }
                break;
            case 'txt':
                file_put_contents($fileName, VarDumper::getDumper()->dumpAsString($data));
                break;
            case 'xml':
                file_put_contents($fileName, XmlFormatHelper::format($data));
                break;
        }
    }
}
