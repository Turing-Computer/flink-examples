package com.flink.pipeline.example7

import com.turing.common.FlinkEnvUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.slf4j.LoggerFactory

object FlinkPipelineKafkaExample {

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

    /// kafka json: {"create_by":"charlie","create_date":"2022-08-08 18:24:23","data_list":[{"name":"zhangsan","age":21}]}
//    val sql =
//      """
//        |CREATE TABLE kafka_source (
//        |    create_by STRING,
//        |    create_date TIMESTAMP(0),
//        |    data_list ARRAY<ROW<
//        |        name    STRING,
//        |        age     INT
//        |        >>
//        |) WITH (
//        |  'connector' = 'kafka',
//        |  'topic' = 'turing-massage-test',
//        |  'properties.group.id' = 'flink-test',
//        |  'properties.bootstrap.servers' ='localhost:9092',
//        |  'format'='json',
//        |  'scan.startup.mode'='earliest-offset'
//        |);
//        |CREATE TABLE sink_table (
//        |    create_by STRING,
//        |    create_date TIMESTAMP(0),
//        |    age ARRAY
//        |) WITH (
//        |  'connector' = 'print'
//        |);
//        |INSERT INTO sink_table
//        |SELECT *
//        |FROM kafka_source
//        |;
//        |""".stripMargin
//    val sqlArr = sql.split(";")

    //
    val sql =
      """
        |CREATE TABLE json_source (
        |    id            BIGINT,
        |    name          STRING,
        |    `date`        DATE,
        |    obj           ROW<time1 TIME,str STRING,lg BIGINT>,
        |    arr           ARRAY<ROW<f1 STRING,f2 INT>>,
        |    `time`        TIME,
        |    `timestamp`   TIMESTAMP(3),
        |    `map`         MAP<STRING,BIGINT>,
        |    mapinmap      MAP<STRING,MAP<STRING,INT>>,
        |    proctime as PROCTIME()
        | ) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'turing-massage-test',
        |  'properties.group.id' = 'flink-test',
        |  'properties.bootstrap.servers' ='localhost:9092',
        |  'format'='json',
        |  'scan.startup.mode'='earliest-offset'
        |);
        |CREATE TABLE sink_table (
        |    id            BIGINT,
        |    name          STRING,
        |    `date`        DATE,
        |    obj           ROW<time1 TIME,str STRING,lg BIGINT>,
        |    arr           ARRAY<ROW<f1 STRING,f2 INT>>,
        |    `time`        TIME,
        |    `timestamp`   TIMESTAMP(3),
        |    `map`         MAP<STRING,BIGINT>,
        |    mapinmap      MAP<STRING,MAP<STRING,INT>>,
        |    proctime     TIMESTAMP(3)
        |) WITH (
        |  'connector' = 'print'
        |);
        |INSERT INTO sink_table
        |SELECT id,name,`date`,obj,arr,`time`,`timestamp`,`map`,mapinmap,proctime
        |FROM json_source
        |;
        |""".stripMargin
    val sqlArr = sql.split(";")

    /**
     * 算子 {@link org.apache.flink.streaming.api.operators.KeyedProcessOperator}
     * --  {@link org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction}
     */
    for (innerSql <- sqlArr) {
      flinkEnv.streamTEnv.executeSql(innerSql)
    }

    flinkEnv.env.execute("FlinkPipelineKafkaExample")

  }
}
