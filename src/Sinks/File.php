<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use DOMException;
use Rabbit\Base\App;
use Rabbit\Base\Core\Exception;
use Rabbit\Base\Exception\InvalidArgumentException;
use Rabbit\Base\Helper\ArrayHelper;
use Rabbit\Base\Helper\ExceptionHelper;
use Rabbit\Base\Helper\FileHelper;
use Rabbit\Base\Helper\VarDumper;
use Rabbit\Base\Helper\XmlHelper;
use Rabbit\Data\Pipeline\AbstractPlugin;
use Rabbit\Data\Pipeline\Message;
use Throwable;

/**
 * Class File
 * @package Rabbit\Data\Pipeline\Sinks
 */
class File extends AbstractPlugin
{
    protected ?string $path;
    protected string $fileName;
    protected string $ext;

    /**
     * @return mixed|void
     * @throws Throwable
     */
    public function init(): void
    {
        parent::init();
        [$configPath, $this->fileName, $this->ext] = ArrayHelper::getValueByArray($this->config, ['path', 'fileName', 'ext']);
        $this->path = App::getAlias($configPath);
    }

    /**
     * @param Message $msg
     * @throws Exception
     * @throws Throwable
     */
    public function run(Message $msg): void
    {
        if (is_array($msg->data)) {
            foreach ($msg->data as $fileName => $data) {
                if (pathinfo($fileName, PATHINFO_DIRNAME)) {
                    $this->saveFile($msg, $fileName, $data);
                } else {
                    $this->saveFile($msg, strtr($this->path, ['{fileName}' => $fileName]), $data);
                }
            }
        } elseif (is_string($msg->data)) {
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
            $this->saveFile($msg, $this->path . '/' . $fileName . ".$this->ext");
        } else {
            throw new InvalidArgumentException("$this->taskName $this->key must input array or string");
        }
    }

    /**
     * @param Message $msg
     * @param string $fileName
     * @param string|null $data
     * @throws Exception
     * @throws Throwable
     * @throws DOMException
     */
    protected function saveFile(Message $msg, string $fileName, string $data = null): void
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
                    foreach (ArrayHelper::toArray($data ?? $msg->data) as $item) {
                        fputcsv($fp, $item);
                    }
                } catch (Throwable $throwable) {
                    App::error(ExceptionHelper::dumpExceptionToString($throwable));
                } finally {
                    fclose($fp);
                }
                break;
            case 'xml':
                $data = $data ?? $msg->data;
                if (is_string($data)) {
                    $this->saveContents($fileName, $data);
                } elseif (is_array($data)) {
                    $this->saveContents($fileName, XmlHelper::format($data));
                }
                break;
            case 'txt':
            default:
                $this->saveContents($fileName, VarDumper::getDumper()->dumpAsString($data));
        }
        $msg->data = $fileName;
        $this->sink($msg);
    }

    /**
     * @param string $fileName
     * @param string|null $data
     * @throws Throwable
     */
    protected function saveContents(string $fileName, ?string $data): void
    {
        $len = file_put_contents($fileName, $data);
        if ($len !== strlen($data)) {
            App::error("save to $fileName $len not enough");
        }
    }
}
