# Nsq消息队列插件

## 介绍

* <big>从Nsq中消费消息</big>

## 依赖

* `rabbit/nsq`

## 配置

* Nsq组件配置

```php
return [
    'nsq' => DI\create(BaseManager::class)
];
```

* 插件配置

```yaml
source_nsq:
  type: Rabbit\Data\Pipeline\Sources\Nsq
  topics:
    name:channel:
        dsn: "localhost:4161"
        dsnd: "localhost:4151"
        pool:
          min: 5
          max: 5

sink_nsq:
  type: Rabbit\Data\Pipeline\Sink\Nsq
  topic: test
  dsn: "localhost:4161"
  dsnd: "localhost:4151"
  pool:
    min: 5
    max: 5
```

* `topic`消息主题，生产者配置
* `topics`消息主题，消费者配置，<font color=red>必填</font>,`[]`类型,key为`主题名称:通道名称`
* `dsn`nsqlookupd http host:port
* `dsnd`nsqd http host:port
* `pool`连接池配置`['min', 'max', 'wait', 'retry']`