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

    val executeSql2 =
      """
        |CREATE TABLE orders (
        |    bidtime TIMESTAMP(3),
        |    price DOUBLE,
        |    item STRING,
        |    supplier STRING,
        |    WATERMARK FOR bidtime AS bidtime - INTERVAL '5' SECONDS
        |) WITH (
        |  'connector' = 'faker',
        |  'fields.bidtime.expression' = '#{date.past ''30'',''SECONDS''}',
        |  'fields.price.expression' = '#{Number.randomDouble ''2'',''1'',''150''}',
        |  'fields.item.expression' = '#{Commerce.productName}',
        |  'fields.supplier.expression' = '#{regexify ''(Alice|Bob|Carol|Alex|Joe|James|Jane|Jack)''}',
        |  'rows-per-second' = '100'
        |);
        |CREATE TABLE sink_table (
        |    window_start TIMESTAMP(3),
        |    window_end TIMESTAMP(3),
        |	   supplier STRING,
        |	   price DOUBLE,
        |    cnt BIGINT,
        |    rownum BIGINT
        |) WITH (
        |  'connector' = 'print'
        |);
        |INSERT INTO sink_table
        |SELECT *
        |    FROM (
        |        SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum
        |        FROM (
        |            SELECT window_start, window_end, supplier, SUM(price) as price, COUNT(*) as cnt
        |            FROM TABLE(
        |                TUMBLE(TABLE orders, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))
        |            GROUP BY window_start, window_end, supplier
        |        )
        |    ) WHERE rownum <= 3;
        |""".stripMargin

    val executeSqlArrary = executeSql2.split(";")
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
