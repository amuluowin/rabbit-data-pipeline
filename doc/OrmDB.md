# OrmDB插件

## 介绍

* <big>swoole_orm中间插件，执行SQL语句</big>

## 依赖

* `rabbit/db-mysql`
* `rabbit/active-record`
* `swoole_orm`

## 配置

* db组件配置

```php
'db' => [
  '{}' => Manager::class
]
```

* 插件配置

```yaml
common_orm:
  type: Rabbit\Data\Pipeline\Common\OrmDB
  sql:
  params:
  query: queryAll
  duration: 60
  dbName: default
  class: \Rabbit\DB\Mysql\Connection
  dsn: 'mysql://root:root@localhost:3306/?dbname=test&charset=utf8'
  pool:
    min: 5
    max: 6
```

* `sql`__参考__:<https://github.com/sethink/swoole-orm>
* `params`参数`[]`
* `query`执行操作方法
* `duration`缓存时间`null不缓存`，`0无限`
* `dbName`获取db配置的连接名，忽略此配置的数据库连接
* `class`数据库连接类
* `dsn`数据库连接dsn
* `pool`连接池配置`['min', 'max', 'wait', 'retry']`