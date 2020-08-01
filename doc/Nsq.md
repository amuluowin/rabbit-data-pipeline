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
  start: true
  topics:
    name:channel:
        dsn: "localhost:4161"
        dsnd: "localhost:4151"
        pool:
          min: 5
          max: 5
```

* `topics`消息主题，<font color=red>必填</font>,`[]`类型,key为`主题名称:通道名称`
* `dsn`nsqlookupd http host:port
* `dsnd`nsqd http host:port
* `pool`连接池配置`['min', 'max', 'wait', 'retry']`