package com.flink.pipeline.example7

import com.turing.common.FlinkEnvUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.table.api.TableEnvironment
import org.slf4j.LoggerFactory
import org.apache.flink.table.api.DataTypes.{ROW, FIELD, BIGINT, STRING, INT}
import org.apache.flink.table.api.{$, EnvironmentSettings, TableEnvironment, row}
import org.apache.flink.table.api.{long2Literal, string2Literal, int2Literal, AnyWithOperations}

object FlinkTableBatchPipelineExample1 {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val fileName = "application-dev.properties"
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val parameterTool = ParameterTool.fromPropertiesFile(inputStream)
    val parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism", "1"))
    val checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url")
    logger.info("load global property file:{}", fileName)

    val tEnv:TableEnvironment  = FlinkEnvUtils.getBatchTableEnv(args).streamTEnv();

    // 定义数据类型
    val MyOrder = ROW(FIELD("id", BIGINT()),
      FIELD("product", STRING()),
      FIELD("amount", INT()))

    val table = tEnv.fromValues(MyOrder, row(1L, "BMW", 1),
      row(2L, "Tesla", 8),
      row(2L, "Tesla", 8),
      row(3L, "BYD", 20))

    val filtered = table.where($("amount").isGreaterOrEqual(8))

    // 调用execute，数据被collect到Job Manager
    filtered.execute().print()

  }

}
