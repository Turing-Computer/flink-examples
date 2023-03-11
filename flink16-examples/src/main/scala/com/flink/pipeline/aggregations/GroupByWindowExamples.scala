package com.flink.pipeline.aggregations

import com.turing.common.FlinkEnvUtils
import com.turing.pipeline.example3.FlinkPipelineExample
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.slf4j.LoggerFactory

object GroupByWindowExamples {

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
        |CREATE TABLE server_logs (
        |    client_ip STRING,
        |    client_identity STRING,
        |    userid STRING,
        |    request_line STRING,
        |    status_code STRING,
        |    log_time AS PROCTIME()
        |) WITH (
        |  'connector' = 'faker',
        |  'fields.client_ip.expression' = '#{Internet.publicIpV4Address}',
        |  'fields.client_identity.expression' =  '-',
        |  'fields.userid.expression' =  '-',
        |  'fields.log_time.expression' =  '#{date.past ''15'',''5'',''SECONDS''}',
        |  'fields.request_line.expression' = '#{regexify ''(GET|POST|PUT|PATCH){1}''} #{regexify ''(/search\.html|/login\.html|/prod\.html|cart\.html|/order\.html){1}''} #{regexify ''(HTTP/1\.1|HTTP/2|/HTTP/1\.0){1}''}',
        |  'fields.status_code.expression' = '#{regexify ''(200|201|204|400|401|403|301){1}''}'
        |);
        |CREATE TABLE sink_table (
        |    ip_addresses BIGINT,
        |    window_interval TIMESTAMP_LTZ(3)
        |) WITH (
        |  'connector' = 'print'
        |);
        |INSERT INTO sink_table
        |SELECT
        |  COUNT(DISTINCT client_ip) AS ip_addresses,
        |  TUMBLE_PROCTIME(log_time, INTERVAL '1' MINUTE) AS window_interval
        |FROM server_logs
        |GROUP BY
        |  TUMBLE(log_time, INTERVAL '1' MINUTE);
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
