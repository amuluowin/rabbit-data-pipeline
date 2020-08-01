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
common_orm:
  type: Rabbit\Data\Pipeline\Common\OrmDB
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
```

* `sql`sql语句或`Query`数组
* `params`参数`[]`
* `query`执行操作方法
* `duration`缓存时间`null不缓存`，`0无限`
* `dbName`获取db配置的连接名，忽略此配置的数据库连接
* `class`数据库连接类
* `dsn`数据库连接dsn
* `pool`连接池配置`['min', 'max', 'wait', 'retry']`