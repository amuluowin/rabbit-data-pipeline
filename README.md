# STS流处理

* <big>基于插件的流程处理组件</big>

## 组件配置

```php
return [
    'scheduler' => DI\create(Scheduler::class)
        ->constructor(
            DI\create(YamlParser::class)->constructor(App::getAlias('@yaml'))
        )
];
```

## 通用配置

```yaml
type: Plugin Class String
start: true|false
output: Plugin Name
errHandler: callable
```

* `type`插件完整类名
* `start`是否开始插件,默认`false`
* `output`输出到下一个插件,支持`Http`输出到远程插件,`worker`多进程任务,格式
  1. `PluginName String|(http|worker):address:PluginName String`，默认不等待
  2. `['PluginName String|(http|worker):address:PluginName String':true|false|int]`
  * PS：`true`启用协程等待，不设超时;`int`启用协程等待，值为超时时间;`false`不等待
* `errHandler`错误处理函数

## 内置插件

### Sources
* [Amqp](./doc/Amqp.md)
* [FindFiles](./doc/FindFiles.md)
* [Http](./doc/Http.md)
* [Nsq](./doc/Nsq.md)
* [Pdo](./doc/pdo.md)
* [RdKafka](./doc/RdKafka.md)

------
### Common
* [HttpRequest](./doc/HttpRequest.md)
* [OrmDB](./doc/OrmDB.md)

------
### TransForms

* [LineParser](./doc/LineParser.md)

------
### Sinks
* [Amqp](./doc/Amqp.md)
* [Clickhouse](.doc/Clickhouse.md)
* [Console](./doc/Console.md)
* [File](./doc/File.md)
* [Nsq](./doc/Nsq.md)
* [Pdo](./doc/PdoSave.md)
* [RdKafka](./doc/RdKafka.md)

### 插件开发

* 继承`AbstractPlugin`
* 实现`run`函数
* 通过`init`从`$this->config`中获取配置参数初始化配置属性
* 插件间传值通过`Message`类，开启协程传递需要`clone Message`，输出值赋值到克隆的`Message->data`属性
* <font color=red>PS：配置属性禁止在运行过程中修改</font>，插件为单例模式，协程环境下会造成属性污染。可使用局部变量替代，例如`$name=$this->name`，使用`$name`