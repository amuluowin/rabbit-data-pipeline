# 文件查找插件

## 介绍

* <big>指定文件或者查找目录下的文件，以文件名作为输出</big>

## 插件配置

```yaml
source_excel:
  type: Rabbit\Data\Pipeline\Sources\FindFiles
  start: true
  fileName: test.xlsl
  scanDir: ./
  extends: 
    - xlsl
```

* `fileName`指定文件位置，默认`null`
* `scanDir`指定扫描目录,默认`[]`
* `extends`指定扫描文件后缀,配合`scanDir`使用,默认`[]`
* `fileName`与`scanDir`有且仅有一种方式