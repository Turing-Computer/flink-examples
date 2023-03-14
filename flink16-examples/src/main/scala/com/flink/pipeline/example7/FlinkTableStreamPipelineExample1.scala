package com.flink.pipeline.example7

import com.turing.common.FlinkEnvUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.table.api.Expressions.{$, lit, row}
import org.apache.flink.table.api.{TableEnvironment}
import org.slf4j.LoggerFactory

object FlinkTableStreeamPipelineExample1$ {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val fileName = "application-dev.properties"
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val parameterTool = ParameterTool.fromPropertiesFile(inputStream)
    val parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism", "1"))
    val checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url")
    logger.info("load global property file:{}", fileName)

    val tEnv:TableEnvironment  = FlinkEnvUtils.getStreamTableEnv(args).streamTEnv();

    val table = tEnv.fromValues(
      row(10, "A"),
      row(20, "A"),
      row(100, "B"),
      row(200, "B")
    ).as("amount", "name")
    tEnv.createTemporaryView("tmp_table", table)

    val table_result = table
      .filter($("amount").isGreater(lit(0)))
      .groupBy($("name"))
      .select($("name"), $("amount").sum().as("amount"))

    table_result.execute().print()

    val sql_result = tEnv.sqlQuery("select name, sum(amount) as amount from tmp_table where amount > 0 group by name")
    sql_result.execute().print()

  }

}
