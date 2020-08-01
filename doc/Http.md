# Http输入插件

## 介绍

* <big>发起Http请求，输出返回内容或者下载文件路径</big>

## 插件配置

```yaml
source_http:
  type: Rabbit\Data\Pipeline\Sources\Http
  uri: www.baidu.com
  retry: 3
  use_pool: true
  ...
```

* `errHandler`错误处理方法
* __其他参考__:<https://github.com/swlib/saber>