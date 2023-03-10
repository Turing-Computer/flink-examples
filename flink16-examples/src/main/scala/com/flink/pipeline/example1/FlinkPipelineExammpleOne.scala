package com.flink.pipeline.example1

import java.io.InputStream
import java.util.Date

import com.turing.bean.ClientLogSource
import com.turing.common.FlinkEnvUtils
import com.turing.pipeline.example3.FlinkPipelineExample
import org.apache.commons.lang3.RandomUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.slf4j.{Logger, LoggerFactory}

/**
 * @descri
 * @author lj.michale
 * @date 2023-03-10
 */
object FlinkPipelineExammpleOne {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val fileName = "application-dev.properties"
    val inputStream = classOf[FlinkPipelineExample].getClassLoader.getResourceAsStream(fileName)
    val parameterTool = ParameterTool.fromPropertiesFile(inputStream)
    val parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism", "1"))
    val checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url")

    logger.info("load global property file:{}", fileName)

    val flinkEnv = FlinkEnvUtils.getStreamTableEnv(args)
    flinkEnv.env.setParallelism(parallelisNum)
    flinkEnv.env.setStateBackend(new EmbeddedRocksDBStateBackend)
    flinkEnv.env.getCheckpointConfig.setCheckpointStorage(checkpointPath)

//    val dataStreamSource = flinkEnv.env.addSource(new RandomStringSource())
//    dataStreamSource.print().setParallelism(1)

    val dataSource: DataStream[List[Double]] = flinkEnv.env.addSource(new CustomGenerator())
    dataSource.print().setParallelism(1)


    flinkEnv.env.execute(this.getClass.getName)

  }


}
