<?php
declare(strict_types=1);

namespace Rabbit\Data\Pipeline\Sinks;

use rabbit\core\ObjectFactory;
use Rabbit\Data\Pipeline\AbstractSingletonPlugin;
use rabbit\exception\InvalidConfigException;
use rabbit\helper\ArrayHelper;
use rabbit\kafka\Broker;
use rabbit\kafka\Producter\Producter;
use rabbit\kafka\Producter\ProducterConfig;
use rabbit\socket\pool\SocketConfig;
use rabbit\socket\pool\SocketPool;
use rabbit\socket\SocketClient;

/**
 * Class Kafka
 * @package Rabbit\Data\Pipeline\Sinks
 */
class Kafka extends AbstractSingletonPlugin
{
    /** @var Producter */
    protected $product;
    /** @var string */
    protected $topic;
    /** @var string */
    protected $kKey = '';

    /**
     * @return mixed|void
     * @throws Exception
     */
    public function init()
    {
        parent::init();
        [
            $dsn,
            $this->topic,
            $pool,
            $options,
            $this->kKey
        ] = ArrayHelper::getValueByArray($this->config, [
            'dsn',
            'topic',
            'pool',
            'options',
            'kKey'
        ], null, [
            'kKey' => $this->kKey,
            'pool' => [],
        ]);
        if (empty($dsn) || empty($this->topic)) {
            throw new InvalidConfigException("dsn & topic must be set!");
        }
        $params = [];
        foreach ($options as $param => $value) {
            $params['set' . lcfirst($param) . '()'] = [$value];
        }
        $this->product = ObjectFactory::createObject([
            'class' => Producter::class,
            'broker' => ObjectFactory::createObject([
                'class' => Broker::class,
                'config' => ObjectFactory::createObject([
                    'class' => ProducterConfig::class,
                ], $params, false),
                'pool' => ObjectFactory::createObject([
                    'class' => SocketPool::class,
                    'poolConfig' => ObjectFactory::createObject([
                        'class' => SocketConfig::class,
                        'uri' => explode(',', $dsn),
                    ], empty($pool) ? [
                        'timeout' => 3,
                        'minActive' => 5,
                        'maxActive' => 6,
                        'maxWait' => 0
                    ] : $pool, false),
                    'client' => SocketClient::class
                ],[],false),
            ], [], false)
        ], [], false);
    }

    public function run()
    {
        $this->product->send([
            [
                'topic' => $this->topic,
                'value' => json_encode($this->input),
                'key' => $this->kKey
            ]
        ]);
    }

}