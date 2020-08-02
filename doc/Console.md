# 控制台输出插件

## 介绍

* <big>输出内容到控制台,一般用于开发调试</big>

## 插件配置

```yaml
sink_console:
  type: Rabbit\Data\Pipeline\Sinks\Console
  encoding: json
  method: echo
```

* `encoding`输出格式，支持`json|xml|html|text`
* `method`输出方式，PHP内置输出函数，如`echo,var_dump,print等`