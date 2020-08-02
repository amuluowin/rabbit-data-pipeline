# 文件输出插件

## 介绍

* <big>将内容写入文件</big>

## 配置

```yaml
sink_file:
  type: Rabbit\Data\Pipeline\Sinks\File
  path: ./
  fileName: test
  ext: txt
```

* `path`文件目录,默认`null`，不配置时通过上层插件输入
* `fileName`文件名,默认`null`,支持`txt|DateTime|Timestamp|callable`，不配置时通过上层插件输入
* `txt`文件后缀,`CSV|XML|JSON`支持自动格式化，不配置时通过上层插件输入