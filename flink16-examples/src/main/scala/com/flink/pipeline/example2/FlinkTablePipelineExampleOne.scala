package com.flink.pipeline.example2

import com.turing.common.FlinkEnvUtils
import com.turing.pipeline.example3.FlinkPipelineExample
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.slf4j.LoggerFactory

object FlinkTablePipelineExampleOne {

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
    //val sql = "CREATE TABLE source_table (\n" + "    user_id BIGINT COMMENT '用户 id',\n" + "    name STRING COMMENT '用户姓名',\n" + "    server_timestamp BIGINT COMMENT '用户访问时间戳',\n" + "    proctime AS PROCTIME()\n" + ") WITH (\n" + "  'connector' = 'datagen',\n" + "  'rows-per-second' = '1',\n" + "  'fields.name.length' = '1',\n" + "  'fields.user_id.min' = '1',\n" + "  'fields.user_id.max' = '10',\n" + "  'fields.server_timestamp.min' = '1',\n" + "  'fields.server_timestamp.max' = '100000'\n" + ");\n" + "\n" + "CREATE TABLE sink_table (\n" + "    user_id BIGINT,\n" + "    name STRING,\n" + "    server_timestamp BIGINT\n" + ") WITH (\n" + "  'connector' = 'print'\n" + ");\n" + "\n" + "INSERT INTO sink_table\n" + "select user_id,\n" + "       name,\n" + "       server_timestamp\n" + "from (\n" + "      SELECT\n" + "          user_id,\n" + "          name,\n" + "          server_timestamp,\n" + "          row_number() over(partition by user_id order by proctime) as rn\n" + "      FROM source_table\n" + ")\n" + "where rn = 1"
    val sql =
      """
        |CREATE TABLE source_table (
        |	user_id BIGINT COMMENT '用户 id',
        |	name STRING COMMENT '用户姓名',
        |	server_timestamp BIGINT COMMENT '用户访问时间戳',
        |	proctime AS PROCTIME()
        |) WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second' = '1',
        |  'fields.name.length' = '1',
        |  'fields.user_id.min' = '1',
        |  'fields.user_id.max' = '10',
        |  'fields.server_timestamp.min' = '1',
        |  'fields.server_timestamp.max' = '100000'
        |);
        |CREATE TABLE sink_table (
        |	user_id BIGINT,
        |	name STRING,
        |	server_timestamp BIGINT
        |) WITH (
        |  'connector' = 'print'
        |);
        |
        |INSERT INTO sink_table
        |select user_id,
        |	   name,
        |	   server_timestamp
        |from (
        |	  SELECT
        |		  user_id,
        |		  name,
        |		  server_timestamp,
        |		  row_number() over(partition by user_id order by proctime) as rn
        |	  FROM source_table
        |)
        |where rn = 1";
        |""".stripMargin

    val sqlArr = sql.split(";")
    /**
     * 算子 {@link org.apache.flink.streaming.api.operators.KeyedProcessOperator}
     * --  {@link org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction}
     */
    for (innerSql <- sqlArr) {
      flinkEnv.streamTEnv.executeSql(innerSql)
    }

    flinkEnv.env.execute(this.getClass.getName)

  }

}
