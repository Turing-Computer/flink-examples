package com.flink.pipeline.example4

import com.turing.common.FlinkEnvUtils
import com.turing.pipeline.example3.FlinkPipelineExample
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.slf4j.LoggerFactory

object FlinkPipelineExample1 {

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

   val executeSql =
     """
       |CREATE TEMPORARY TABLE location_updates (
       |  `character_id` INT,
       |  `location` STRING,
       |  `proctime` AS PROCTIME()
       |)
       |WITH (
       |  'connector' = 'faker',
       |  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',
       |  'fields.location.expression' = '#{harry_potter.location}'
       |);
       |CREATE TEMPORARY TABLE characters (
       |  `character_id` INT,
       |  `name` STRING
       |)
       |WITH (
       |  'connector' = 'faker',
       |  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',
       |  'fields.name.expression' = '#{harry_potter.characters}'
       |);
       |CREATE TABLE sink_table (
       |    character_id INT,
       |    location STRING,
       |    name STRING
       |) WITH (
       |  'connector' = 'print'
       |);
       |INSERT INTO sink_table
       |SELECT
       |  c.character_id,
       |  l.location,
       |  c.name
       |FROM location_updates AS l
       |JOIN characters FOR SYSTEM_TIME AS OF proctime AS c
       |ON l.character_id = c.character_id;
       |""".stripMargin

    val executeSqlArrary = executeSql.split(";")

    /**
     * 算子 {@link org.apache.flink.streaming.api.operators.KeyedProcessOperator}
     * --  {@link org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction}
     */
    for (innerSql <- executeSqlArrary) {
        flinkEnv.streamTEnv.executeSql(innerSql)
    }

    flinkEnv.env.execute("FlinkPipelineExample")

  }

}
