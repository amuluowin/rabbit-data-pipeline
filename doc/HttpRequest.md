# Http中间插件

## 介绍

* <big>发起Http请求，输出返回内容或者下载文件路径</big>

## 插件配置

```yaml
common_http:
  type: Rabbit\Data\Pipeline\Common\HttpRequest
  usePool: true
  timeout: 30
  throttleTime: 60
  retry: callable
  checkResponseFunc: callable
  format: jsonArray
  download: false
```

* `usePool`连接池数量
* `timeout`超时时间
* `throttleTime`限频间隔
* `retry`重试拦截处理函数
* `checkResponseFunc`校验返回值函数
* `format`格式化方式 `null|jsonArray|jsonObject|xmlArray|xmlObject|domObject|xpathObject`
* `download`是否下载