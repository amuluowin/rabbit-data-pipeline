# Kafka插件

## 介绍

* <big>Kafka消费者插件</big>

## 依赖

* `rabbit/rdkafka`
* `php-rdkafka`

## 配置

* Kafka组件配置

```php
return [
    'kafka' => [
      '{}' => KafkaManager::class
    ]
];
```

* 插件配置

```yaml
source_kafka:
  type: Rabbit\Data\Pipeline\Sources\Kafka
  start: true
  topic: test
  dsn: localhost:9092, localhost:9093, localhost:9094
  options:
    GroupId: ck1
    BrokerVersion: 1.0.0
    OffsetReset: earliest
    MetadataRefreshIntervalMs: 10000

sink_kafka:
  type: Rabbit\Data\Pipeline\Sinks\Kafka
  topic: test
  topicSet: 
    acks: '0'
  dsn: localhost:9092, localhost:9093, localhost:9094
  options:
    socket.keepalive.enable: 'true'
```

* `topic`消息主题
* `dsn`kaaka服务器地址
* `options & topicSet`__参考__:<https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md>