<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use Rabbit\Base\App;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Exception\InvalidConfigException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Base\Helper\FileHelper;
use Rabbit\Base\Helper\VarDumper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Throwable;

/**
 * Class File
 * @package Rabbit\Data\Pipeline\Sinks
 */
class File extends AbstractPlugin
{
    /** @var string */
    protected ?string $path;
    /** @var string */
    protected string $fileName;
    /** @var string */
    protected string $ext;

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init()
    {
        parent::init();
        [$configPath, $this->fileName, $this->ext] = ArrayHelper::getValueByArray($this->config, ['path', 'fileName', 'ext']);
        $this->path = App::getAlias($configPath);
    }

    /**
     * @throws Exception
     * @throws Throwable
     */
    public function run(): void
    {
        if (is_array($this->input)) {
            foreach ($this->input as $fileName => $data) {
                if (pathinfo($fileName, PATHINFO_DIRNAME)) {
                    $this->saveFile($fileName, $data);
                } else {
                    $this->saveFile(strtr($this->path, ['{fileName}' => $fileName]), $data);
                }
            }
        } elseif (is_string($this->input)) {
            if (is_callable($this->fileName)) {
                $fileName = call_user_func($this->fileName);
            } else {
                switch ($this->fileName) {
                    case "DateTime":
                        $fileName = date('YmdHis', time());
                        break;
                    case "Timestamp":
                        $fileName = time();
                        break;
                    default:
                        $fileName = $this->fileName;
                }
            }
            $this->saveFile($this->path . '/' . $fileName . ".$this->ext");
        } else {
            throw new InvalidArgumentException("$this->taskName $this->key must input array or string");
        }
    }

    /**
     * @param string $fileName
     * @param string $data
     * @throws Exception
     * @throws Throwable
     */
    protected function saveFile(string $fileName, string $data = null): void
    {
        FileHelper::createDirectory(dirname($fileName), 777);
        $ext = strtolower(pathinfo($fileName, PATHINFO_EXTENSION));
        switch ($ext) {
            case 'csv':
                if (false === $fp = fopen($fileName, 'w+')) {
                    App::error("can not open file $fileName");
                    return;
                }
                try {
                    foreach (ArrayHelper::toArray($data ?? $this->input) as $item) {
                        fputcsv($fp, $item);
                    }
                } catch (Throwable $throwable) {
                    App::error(ExceptionHelper::dumpExceptionToString($throwable));
                } finally {
                    fclose($fp);
                }
                break;
            case 'xml':
                $data = $data ?? $this->input;
                if (is_string($data)) {
                    $this->saveContents($fileName, $data);
                } else {
                    $this->saveContents($fileName, XmlFormatHelper::format($data));
                }
                break;
            case 'txt':
            default:
                $this->saveContents($fileName, VarDumper::getDumper()->dumpAsString($data));
        }
        $this->output($fileName);
    }

    /**
     * @param string $fileName
     * @param string|null $data
     * @throws Throwable
     */
    protected function saveContents(string $fileName, ?string $data): void
    {
        $len = file_put_contents($fileName, $this->input);
        if ($len !== strlen($this->input)) {
            App::error("save to $fileName $len not enough");
        }
    }
}
