# Pdo插件

## 介绍

* <big>Pdo驱动从数据库获取数据</big>

## 依赖

* `rabbit/db-mysql`
* `rabbit/active-record`

## 配置

* db组件配置

```php
'db' => DI\create(Manager::class)
```

* 插件配置

```yaml
source_pdo:
  type: Rabbit\Data\Pipeline\Sources\Pdo
  sql: select * from test
  params:
  query: queryAll
  duration: 60
  dbName: default
  class: \Rabbit\DB\Mysql\Connection
  dsn: 'mysql://root:root@localhost:3306/?dbname=test&charset=utf8'
  pool:
    min: 5
    max: 6

sink_pdo:
  type: Rabbit\Data\Pipeline\Sinks\Pdo
  tableName: test
  dbName: default
  class: \Rabbit\DB\Mysql\Connection
  dsn: 'mysql://root:root@localhost:3306/?dbname=test&charset=utf8'
  pool:
    min: 5
    max: 6
```

* `tableName`要写入的数据库表,<big>sink插件用</big>
* `sql`sql语句或`Query`数组,<big>source插件用</big>
* `params`参数`[]`,<big>source插件用</big>
* `query`执行操作方法,<big>source插件用</big>
* `duration`缓存时间`null不缓存`，`0无限`,<big>source插件用</big>
* `dbName`获取db配置的连接名，忽略此配置的数据库连接
* `class`数据库连接类
* `dsn`数据库连接dsn
* `pool`连接池配置`['min', 'max', 'wait', 'retry']`