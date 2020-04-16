<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Common;

use rabbit\App;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\httpserver\CoServer;
use rabbit\server\Server;

/**
 * Class StartAll
 * @package Rabbit\Data\Pipeline\Common
 */
class StartAll extends AbstractSingletonPlugin
{
    public function run()
    {
        $server = App::getServer();
        if ($server === null) {
            $workers = getDI('socketHandle')->getWorkerIds();
        } elseif ($server instanceof CoServer) {
            $workers = $server->getProcessSocket()->getWorkerIds();
        } elseif ($server instanceof Server) {
            $workers = range(0, $server->getSwooleServer()->set(['worker_num']) - 1);
        }
        foreach ($workers as $id) {
            $cid = \Co::getCid();
            if ($id === 0) {
                array_walk($this->output, function (&$value) {
                    $value = false;
                });
                rgo(function () use ($cid) {
                    $this->output($cid);
                });
            } else {
                rgo(function () use ($cid, $id) {
                    $this->output($cid, $id);
                });
            }
        }
    }

}