# Clickhouse输出

## 介绍

* <big>Clickhouse输出插件，自带`flag`更新去重算法</big>

## 依赖

* `rabbit/db-clickhouse` or `rabbit/db-click`

## 配置

* 组件配置

```php
'clickhouse' => DI\create(BaseManager::class),
'click' => DI\create(Manager::class)
```

* 插件配置

```yaml
sink_clickhouse:
  type: Rabbit\Data\Pipeline\Sinks\Clickhouse
  dbName: default
  driver: clickhouse
  class: \Rabbit\DB\Clickhouse\Connection
  dsn: 'clickhouse://root:root@localhost:8123/?dbname=test'
  pool:
    min: 5
    max: 6
  tableName: test
  primaryKey: 
    - id
  flagField: flag
```

* `dbName`获取db配置的连接名，设置后会忽略此配置的数据库连接
* `driver`驱动名，设置后会忽略此配置的数据库连接
* `class`数据库连接类
* `dsn`数据库连接dsn
* `pool`连接池配置`['min', 'max', 'wait', 'retry']`,仅对`click`驱动生效
* `tableName`需要写入的表名,未配置时，需要由上层插件输入
* `primaryKey`唯一索引,默认`[id]`
* `flagField`更新算法的标识位`0新数据，不可读;1可读;2旧数据;3已删除`