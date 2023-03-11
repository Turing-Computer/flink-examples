package com.flink.pipeline.example4

import com.turing.common.FlinkEnvUtils
import com.turing.pipeline.example3.FlinkPipelineExample
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.slf4j.LoggerFactory

object FlinkPipelineExample {

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

    val sql = "CREATE TEMPORARY TABLE datagen_source (\n" + "  `character_id` INT,\n" + "  `location` STRING,\n" + "  `proctime` AS PROCTIME()\n" + ") WITH (\n" + "  'connector' = 'datagen'\n" + ");\n" + "\n" + "CREATE TEMPORARY TABLE faker_dim (\n" + "  `character_id` INT,\n" + "  `name` STRING\n" + ") WITH (\n" + "  'connector' = 'faker',\n" + "  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',\n" + "  'fields.name.expression' = '#{harry_potter.characters}'\n" + ");\n" + "\n" + "CREATE TABLE sink_table (\n" + "    character_id INT,\n" + "    location STRING,\n" + "    name STRING\n" + ") WITH (\n" + "  'connector' = 'print'\n" + ");\n" + "INSERT INTO sink_table\n" + "SELECT\n" + "  c.character_id,\n" + "  l.location,\n" + "  c.name\n" + "FROM datagen_source AS l\n" + "JOIN faker_dim FOR SYSTEM_TIME AS OF proctime AS c\n" + "ON l.character_id = c.character_id;"
    //    val sql = ""
    val sqlArr = sql.split(";")

    /**
     * 算子 {@link org.apache.flink.streaming.api.operators.KeyedProcessOperator}
     * --  {@link org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction}
     */
    for (innerSql <- sqlArr) {
        flinkEnv.streamTEnv.executeSql(innerSql)
    }

    flinkEnv.env.execute("FlinkPipelineExample")

  }

}
