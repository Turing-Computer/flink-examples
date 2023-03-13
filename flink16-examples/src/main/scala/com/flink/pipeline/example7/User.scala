package com.flink.pipeline.example7

import org.apache.flink.table.annotation.{DataTypeHint, HintFlag}

class User {

  //将java.lang.Integer类转化为 INT 数据类型
  @DataTypeHint("INT")
  var o: AnyRef

  // 使用显式转换类定义毫秒精度的TIMESTAMP数据类型
  @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class)
  var o: AnyRef

  //强制使用RAW类型来丰富类型提取
  @DataTypeHint("RAW")
  var modelClass: Class[_]

  // 定义所有出现的java.math.BigDecimal(也包括嵌套字段)将被提取为DECIMAL(12, 2)
  @DataTypeHint(defaultDecimalPrecision = 12, defaultDecimalScale = 2)
  var stmt: AccountStatement

  // 定义每当类型不能映射到数据类型时，不要抛出异常，而应始终将其视为RAW类型
  @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
  var model: ComplexModel

}
