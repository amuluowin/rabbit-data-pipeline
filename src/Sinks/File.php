<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use common\Helpers\FileHelper;
use common\Helpers\XmlFormatHelper;
use rabbit\App;
use rabbit\core\Exception;
use Rabbit\Data\Pipeline\AbstractPlugin;
use rabbit\exception\InvalidArgumentException;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\helper\ExceptionHelper;
use rabbit\helper\VarDumper;

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
    /** @var string */
    protected $ext;

    /**
     * @return mixed|void
     * @throws InvalidConfigException
     */
    public function init()
    {
        parent::init();
        [$configPath, $this->fileName, $this->ext] = ArrayHelper::getValueByArray($this->config, ['path', 'fileName', 'ext']);
        $this->path = App::getAlias($configPath);
    }


    /**
     * @param null $input
     * @param array $opt
     * @throws Exception
     */
    public function input(&$input = null, &$opt = []): void
    {
        if (is_array($input)) {
            foreach ($input as $fileName => $data) {
                if (pathinfo($fileName, PATHINFO_DIRNAME)) {
                    $this->saveFile($fileName, $data);
                } else {
                    $this->saveFile(strtr($this->path, ['{fileName}' => $fileName]), $data);
                }
            }
        } elseif (is_string($input)) {
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
            $this->saveFile($this->path . '/' . $fileName . ".$this->ext", $input);
        } else {
            throw new InvalidArgumentException("$this->taskName $this->key must input array or string");
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
                $this->saveContents($fileName, VarDumper::getDumper()->dumpAsString($data));
                break;
            case 'xml':
                if (is_string($data)) {
                    $this->saveContents($fileName, $data);
                } else {
                    $this->saveContents($fileName, XmlFormatHelper::format($data));
                }
                break;
        }
        $this->output($fileName);
    }

    protected function saveContents(string $fileName, string $data): void
    {
        $len = file_put_contents($fileName, $data);
        if ($len !== strlen($data)) {
            App::error("save to $fileName $len not enough");
        }
    }
}
