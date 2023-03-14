package com.flink.pipeline.example7

import com.turing.common.FlinkEnvUtils
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.slf4j.LoggerFactory

object FlinkTableStreamPipelineByStreamTableEnvironmentDemo {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val fileName = "application-dev.properties"
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val parameterTool = ParameterTool.fromPropertiesFile(inputStream)
    val parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism", "1"))
    val checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url")
    logger.info("load global property file:{}", fileName)

    val flinkEnv:StreamExecutionEnvironment = FlinkEnvUtils.getStreamTableEnv(args).getStreamExecutionEnvironment
    flinkEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    val tEnv = StreamTableEnvironment.create(flinkEnv)

    flinkEnv.execute(s"${this.getClass.getName}")

  }

}
