# 文档转换插件

## 介绍

* <big>`TXT,CSV,Excel`文档读取转换</big>

## 配置

```yaml
trans_parse:
  type: Rabbit\Data\Pipeline\Transforms\LineParser
  fileType: xlsx
  headLine: 3
  dataLine:
    - 5
  include:
    0: null
    2: null
    6: return '20'.explode(' ',$col)[0];
    23: null
  sheet: 每日统计
  map:
    姓名: user_name
    部门: department
    日期: work_date
    工作时长(分钟): woke_time
```

* `fileType`文件类型,支持`TXT,CSV,Excel`
* `headLine`标题行，默认`null`，下标从`1`开始
* `dataLine`内容行`[]`类型,默认`[1]`，下标从`1`开始
* `split`行分隔符,默认`PHP_EOF`,`TXT`类型用
* `explode`列分隔符,默认`\t`,`TXT`类型用
* `endLine`最后一行位置,默认`null`,读取全部行
* `field`特殊行字段配置`[]`类型，<font color=red>有些文件为了减少体积会把相同内容列提取为一行</font>
* `fieldLine`特殊字段行位置，配合`field`使用
* `delimiter`默认`','`参考`fgetcsv`参数
* `enclosure`默认`'"'`参考`fgetcsv`参数
* `escape`默认`'\\'`参考`fgetcsv`参数
* `include`包含的列,默认`[]`,读取全部列,格式`[index=>code]`,通过`eval`调用,`$col`为当前列的值
* `exclude`排除列,默认`[]`,不排除
* `map`列映射，主要用于映射数据库字段,格式`['fileCol'=>'dbCol']`
* `sheet`需要读取的`Excel`工作簿
* `addField`固定添加的列,默认`[]`,格式为`['name'=>value]`