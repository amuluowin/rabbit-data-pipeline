# STS流处理

* 插件式流程处理组件

## 通用配置

```yaml
type: 
start: 
output:
wait:
```

`type`插件类名
`start`是否开始插件
`output`输出到下一个插件
`errHandler`错误处理函数

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
* 插件间传值通过`Message`类，开启协程传递需要`clone Message`,输出值赋值到克隆的`Message->data`属性
* <font color=red>注意:配置属性禁止在运行过程中修改</font>，插件为单例模式，协程环境下会造成属性污染