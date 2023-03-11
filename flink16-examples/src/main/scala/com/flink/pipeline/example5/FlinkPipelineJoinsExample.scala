package com.flink.pipeline.example5

import com.turing.common.FlinkEnvUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.slf4j.LoggerFactory

object FlinkPipelineJoinsExample {

  private val logger = LoggerFactory.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val fileName = "application-dev.properties"
    val inputStream = this.getClass.getClassLoader.getResourceAsStream(fileName)
    val parameterTool = ParameterTool.fromPropertiesFile(inputStream)
    val parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism", "1"))
    val checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url")
    logger.info("load global property file:{}", fileName)

    val flinkEnv = FlinkEnvUtils.getStreamTableEnv(args)
    flinkEnv.env.setParallelism(parallelisNum)
    flinkEnv.env.setStateBackend(new EmbeddedRocksDBStateBackend)
    flinkEnv.env.getCheckpointConfig.setCheckpointStorage(checkpointPath)

    // Regular Joins
//    val sql =
//      """
//        |CREATE TABLE NOC (
//        |  agent_id STRING,
//        |  codename STRING
//        |)
//        |WITH (
//        |  'connector' = 'faker',
//        |  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',
//        |  'fields.codename.expression' = '#{superhero.name}',
//        |  'number-of-rows' = '10'
//        |);
//        |
//        |CREATE TABLE RealNames (
//        |  agent_id STRING,
//        |  name     STRING
//        |)
//        |WITH (
//        |  'connector' = 'faker',
//        |  'fields.agent_id.expression' = '#{regexify ''(1|2|3|4|5){1}''}',
//        |  'fields.name.expression' = '#{Name.full_name}',
//        |  'number-of-rows' = '10'
//        |);
//        |CREATE TABLE sink_table (
//        |    name STRING,
//        |    codename STRING
//        |) WITH (
//        |  'connector' = 'print'
//        |);
//        |INSERT INTO sink_table
//        |SELECT
//        |    name,
//        |    codename
//        |FROM NOC
//        |INNER JOIN RealNames ON NOC.agent_id = RealNames.agent_id;
//        |""".stripMargin

    val sql =
      """
        |CREATE TABLE orders (
        |  id INT,
        |  order_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)+5)*(-1) AS INT), CURRENT_TIMESTAMP)
        |)
        |WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second'='10',
        |  'fields.id.kind'='sequence',
        |  'fields.id.start'='1',
        |  'fields.id.end'='1000'
        |);
        |CREATE TABLE shipments (
        |  id INT,
        |  order_id INT,
        |  shipment_time AS TIMESTAMPADD(DAY, CAST(FLOOR(RAND()*(1-5+1)) AS INT), CURRENT_TIMESTAMP)
        |)
        |WITH (
        |  'connector' = 'datagen',
        |  'rows-per-second'='5',
        |  'fields.id.kind'='random',
        |  'fields.id.min'='0',
        |  'fields.order_id.kind'='sequence',
        |  'fields.order_id.start'='1',
        |  'fields.order_id.end'='1000'
        |);
        |CREATE TABLE sink_table (
        |    order_id INT,
        |	   order_time TIMESTAMP_LTZ(3),
        |	   shipment_time TIMESTAMP_LTZ(3),
        |    day_diff INT
        |) WITH (
        |  'connector' = 'print'
        |);
        |INSERT INTO sink_table
        |SELECT
        |  o.id AS order_id,
        |  o.order_time,
        |  s.shipment_time,
        |  TIMESTAMPDIFF(DAY,o.order_time,s.shipment_time) AS day_diff
        |FROM orders o
        |JOIN shipments s ON o.id = s.order_id
        |WHERE
        |    o.order_time BETWEEN s.shipment_time - INTERVAL '3' DAY AND s.shipment_time;
        |""".stripMargin
    val sqlArr = sql.split(";")

    /**
     * 算子 {@link org.apache.flink.streaming.api.operators.KeyedProcessOperator}
     * --  {@link org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction}
     */
    for (innerSql <- sqlArr) {
        flinkEnv.streamTEnv.executeSql(innerSql)
    }

    flinkEnv.env.execute("FlinkPipelineJoinsExample")

  }

}
