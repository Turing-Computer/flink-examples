package com.flink.pipeline.example6

import com.turing.common.FlinkEnvUtils
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.slf4j.LoggerFactory

object FlinkPipelineKafkaToHiveExample {

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

    /**
     * 写入无分区表
     * hive 表信息
     * CREATE TABLE `test.order_info`(
     * `id` int COMMENT '订单id',
     * `product_count` int COMMENT '购买商品数量',
     * `one_price` double COMMENT '单个商品价格')
     * ROW FORMAT SERDE
     * 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     * STORED AS INPUTFORMAT
     * 'org.apache.hadoop.mapred.TextInputFormat'
     * OUTPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
     * LOCATION
     * 'hdfs://hadoopCluster/user/hive/warehouse/test.db/order_info'
     * TBLPROPERTIES (
     * 'transient_lastDdlTime'='1659250044')
     * ;
     */
    val sql =
      """
        | -- 如果是 flink-1.13.x ，则需要手动设置该参数
        |set 'table.dynamic-table-options.enabled' = 'true';
        |
        |-- 在需要读取hive或者是写入hive表时，必须创建hive catalog。
        |-- 创建catalog
        |create catalog hive with (
        |    'type' = 'hive',
        |    'hive-conf-dir' = 'hdfs:///hadoop-conf'
        |)
        |;
        |
        |use catalog hive;
        |
        |-- 创建连接 kafka 的虚拟表作为 source，此处使用 temporary ，是为了不让创建的虚拟表元数据保存到 hive，可以让任务重启是不出错。
        |-- 如果想让虚拟表元数据保存到 hive ，则可以在创建语句中加入 if not exist 语句。
        |CREATE temporary TABLE source_kafka(
        |    id integer comment '订单id',
        |    product_count integer comment '购买商品数量',
        |    one_price double comment '单个商品价格'
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'data_gen_source',
        |    'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',
        |    'properties.group.id' = 'for_source_test',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |insert into test.order_info
        |-- 下面的语法是 flink sql 提示，用于在语句中使用到表时手动设置一些临时的参数
        |/*+
        |OPTIONS(
        |    -- 设置写入的文件滚动时间间隔
        |    'sink.rolling-policy.rollover-interval' = '10 s',
        |    -- 设置检查文件是否需要滚动的时间间隔
        |    'sink.rolling-policy.check-interval' = '1 s',
        |    -- sink 并行度
        |    'sink.parallelism' = '1'
        |)
        | */
        |select id, product_count, one_price
        |from source_kafka
        |;
        |""".stripMargin


    /**
     * 写入分区表
     * hive 表信息
     * CREATE TABLE `test.order_info_have_partition`(
     * `product_count` int COMMENT '购买商品数量',
     * `one_price` double COMMENT '单个商品价格')
     * PARTITIONED BY (
     * `minute` string COMMENT '订单时间，分钟级别',
     * `order_id` int COMMENT '订单id')
     * ROW FORMAT SERDE
     * 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     * STORED AS INPUTFORMAT
     * 'org.apache.hadoop.mapred.TextInputFormat'
     * OUTPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
     * LOCATION
     * 'hdfs://hadoopCluster/user/hive/warehouse/test.db/order_info_have_partition'
     * TBLPROPERTIES (
     * 'transient_lastDdlTime'='1659254559')
     * ;
     */
    val sql2 =
      """
        |-- 如果是 flink-1.13.x ，则需要手动设置该参数
        |set 'table.dynamic-table-options.enabled' = 'true';
        |
        |-- 在需要读取hive或者是写入hive表时，必须创建hive catalog。
        |-- 创建catalog
        |create catalog hive with (
        |    'type' = 'hive',
        |    'hive-conf-dir' = 'hdfs:///hadoop-conf'
        |)
        |;
        |
        |use catalog hive;
        |-- 创建连接 kafka 的虚拟表作为 source，此处使用 temporary ，是为了不让创建的虚拟表元数据保存到 hive，可以让任务重启是不出错。
        |-- 如果想让虚拟表元数据保存到 hive ，则可以在创建语句中加入 if not exist 语句。
        |CREATE temporary TABLE source_kafka(
        |    event_time TIMESTAMP(3) METADATA FROM 'timestamp',
        |    id integer comment '订单id',
        |    product_count integer comment '购买商品数量',
        |    one_price double comment '单个商品价格'
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'data_gen_source',
        |    'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',
        |    'properties.group.id' = 'for_source_test',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |insert into test.order_info_have_partition
        |-- 下面的语法是 flink sql 提示，用于在语句中使用到表时手动设置一些临时的参数
        |/*+
        |OPTIONS(
        |    -- 设置分区提交触发器为分区时间
        |    'sink.partition-commit.trigger' = 'partition-time',
        |--     'partition.time-extractor.timestamp-pattern' = '$year-$month-$day $hour:$minute',
        |    -- 设置时间提取器的时间格式，要和分区字段值的格式保持一直
        |    'partition.time-extractor.timestamp-formatter' = 'yyyy-MM-dd_HH:mm',
        |    -- 设置分区提交延迟时间，这儿设置 1 分钟，是因为分区时间为 1 分钟间隔
        |    'sink.partition-commit.delay' = '1 m',
        |    -- 设置水印时区
        |    'sink.partition-commit.watermark-time-zone' = 'GMT+08:00',
        |    -- 设置分区提交策略，这儿是将分区提交到元数据存储，并且在分区目录下生成 success 文件
        |    'sink.partition-commit.policy.kind' = 'metastore,success-file',
        |    -- sink 并行度
        |    'sink.parallelism' = '1'
        |)
        | */
        |select
        |    product_count,
        |    one_price,
        |    -- 不要让分区值中带有空格，分区值最后会变成目录名，有空格的话，可能会有一些未知问题
        |    date_format(event_time, 'yyyy-MM-dd_HH:mm') as `minute`,
        |    id as order_id
        |from source_kafka
        |;
        |""".stripMargin

    // hive -> hive
    /**
     * hive 表信息
     * CREATE TABLE `test.source_table`(
     * `col1` string,
     * `col2` string)
     * ROW FORMAT SERDE
     * 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     * STORED AS INPUTFORMAT
     * 'org.apache.hadoop.mapred.TextInputFormat'
     * OUTPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
     * LOCATION
     * 'hdfs://hadoopCluster/user/hive/warehouse/test.db/source_table'
     * TBLPROPERTIES (
     * 'transient_lastDdlTime'='1659260162')
     * ;
     *
     * CREATE TABLE `test.sink_table`(
     * `col1` string,
     * `col2` array<string> comment '保存 collect_list 函数的结果',
     * `col3` array<string> comment '保存 collect_set 函数的结果')
     * ROW FORMAT SERDE
     * 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
     * STORED AS INPUTFORMAT
     * 'org.apache.hadoop.mapred.TextInputFormat'
     * OUTPUTFORMAT
     * 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
     * LOCATION
     * 'hdfs://hadoopCluster/user/hive/warehouse/test.db/sink_table'
     * TBLPROPERTIES (
     * 'transient_lastDdlTime'='1659260374')
     * ;
     *
     * 下面将演示两种 sql 方言，将 source_table 表数据，写入 sink_table 表，并且呈现上面图示的结果
     */
    val hiveSql =
      """
        |set 'table.local-time-zone' = 'GMT+08:00';
        |
        |-- 在需要读取hive或者是写入hive表时，必须创建hive catalog。
        |-- 创建catalog
        |create catalog hive with (
        |    'type' = 'hive',
        |    'hive-conf-dir' = 'hdfs:///hadoop-conf'
        |)
        |;
        |
        |use catalog hive;
        |-- 加载 hive module 之后，flink 就会将 hive 模块放到模块解析顺序的最后。
        |-- 之后flink 引擎会自动使用 hive 模块来解析 flink 模块解析不了的函数，如果想改变模块解析顺序，则可以使用 use modules hive, core; 语句来改变模块解析顺序。
        |load module hive;
        |
        |insert overwrite test.sink_table
        |select col1, collect_list(col2) as col2, collect_set(col2) as col3
        |from test.source_table
        |group by col1
        |;
        |""".stripMargin
    val sqlArr = sql.split(";")

    val hiveSql2 =
      """
        |set 'table.local-time-zone' = 'GMT+08:00';
        |
        |-- 在需要读取hive或者是写入hive表时，必须创建hive catalog。
        |-- 创建catalog
        |create catalog hive with (
        |    'type' = 'hive',
        |    'hive-conf-dir' = 'hdfs:///hadoop-conf'
        |)
        |;
        |
        |use catalog hive;
        |
        |-- 加载 hive module 之后，flink 就会将 hive 模块放到模块解析顺序的最后。
        |-- 之后flink 引擎会自动使用 hive 模块来解析 flink 模块解析不了的函数，如果想改变模块解析顺序，则可以使用 use modules hive, core; 语句来改变模块解析顺序。
        |load module hive;
        |
        |-- 切记，设置方言之后，之后所有的语句将使用你手动设置的方言进行解析运行
        |-- 这儿设置了使用 hive 方言，因此下面的 insert 语句就可以直接使用 hive sql 方言了，也就是说，下面可以直接运行 hive sql 语句。
        |set 'table.sql-dialect' = 'hive';
        |
        |-- insert overwrite `table_name` 是 flink sql 方言语法
        |-- insert overwrite table `table_name` 是 hive sql 方言语法
        |insert overwrite table test.sink_table
        |select col1, collect_list(col2) as col2, collect_set(col2) as col3
        |from test.source_table
        |group by col1
        |;
        |
        |""".stripMargin


    // temporal join(时态连接)
    val temporalJoinSql =
      """
        |set 'table.local-time-zone' = 'GMT+08:00';
        |-- 如果 source kafka 主题中有些分区没有数据，就会导致水印无法向下游传播，此时需要手动设置空闲时间
        |set 'table.exec.source.idle-timeout' = '1 s';
        |
        |-- 订单流水
        |CREATE temporary TABLE order_flow(
        |    id int comment '订单id',
        |    product_count int comment '购买商品数量',
        |    one_price double comment '单个商品价格',
        |    -- 定义订单时间为数据写入 kafka 的时间
        |    order_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
        |    WATERMARK FOR order_time AS order_time
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'data_gen_source',
        |    'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',
        |    'properties.group.id' = 'for_source_test',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |-- 订单信息
        |create table order_info (
        |    id int PRIMARY KEY NOT ENFORCED comment '订单id',
        |    user_name string comment '订单所属用户',
        |    order_source string comment '订单所属来源',
        |    update_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL,
        |    WATERMARK FOR update_time AS update_time
        |) with (
        |    'connector' = 'upsert-kafka',
        |    'topic' = 'order_info',
        |    'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',
        |    'key.format' = 'csv',
        |    'value.format' = 'csv',
        |    'value.csv.field-delimiter' = ' '
        |)
        |;
        |
        |-- 创建连接 kafka 的虚拟表作为 sink
        |create table sink_kafka(
        |    id int PRIMARY KEY NOT ENFORCED comment '订单id',
        |    user_name string comment '订单所属用户',
        |    order_source string comment '订单所属来源',
        |    product_count int comment '购买商品数量',
        |    one_price double comment '单个商品价格',
        |    total_price double comment '总价格'
        |) with (
        |    'connector' = 'upsert-kafka',
        |    'topic' = 'for_sink',
        |    'properties.bootstrap.servers' = 'node01:9092,node02:9092,node03:9092',
        |    'key.format' = 'csv',
        |    'value.format' = 'csv',
        |    'value.csv.field-delimiter' = ' '
        |)
        |;
        |
        |-- 真正要执行的任务
        |insert into sink_kafka
        |select
        |    order_flow.id,
        |    order_info.user_name,
        |    order_info.order_source,
        |    order_flow.product_count,
        |    order_flow.one_price,
        |    order_flow.product_count * order_flow.one_price as total_price
        |from order_flow
        |left join order_info FOR SYSTEM_TIME AS OF order_flow.order_time
        |on order_flow.id = order_info.id
        |;
        |
        |""".stripMargin

    ///kafka join JDBC
    val kafkaJoinJdbcSql =
      """
        |create table source (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价'
        |) with (
        |    'connector' = 'kafka',
        |    'topic' = 'source1',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'scan.startup.mode' = 'latest-offset',
        |    'properties.group.id' = 'test',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |create table dim_goods (
        |    goods_id integer comment '商品id',
        |    goods_name string comment '商品名称'
        |) with (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://hadoop01:3306/test',
        |    'username' = 'test',
        |    'password' = 'test',
        |    'table-name' = 'dim_goods'
        |)
        |;
        |
        |create table sink (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_name string comment '商品名称',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价'
        |) with (
        |    'connector' = 'kafka',
        |    'topic' = 'sink1',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |insert into sink
        |select id, a.goods_id, b.goods_name, a.goods_count, a.price_total
        |from source as a
        |join dim_goods as b
        |on a.goods_id = b.goods_id
        |;
        |
        |""".stripMargin

    // lookup join
    val kafkaJoinJdbcSql2 =
      """
        |set pipeline.operator-chaining = false;
        |
        |create table source (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价',
        |    proctime as proctime()
        |) with (
        |    'connector' = 'kafka',
        |    'topic' = 'source1',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'scan.startup.mode' = 'latest-offset',
        |    'properties.group.id' = 'test',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |create table dim_goods (
        |    goods_id integer comment '商品id',
        |    goods_name string comment '商品名称'
        |) with (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://hadoop01:3306/test',
        |    'username' = 'test',
        |    'password' = 'test',
        |    'table-name' = 'dim_goods'
        |)
        |;
        |
        |create table sink (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_name string comment '商品名称',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价'
        |) with (
        |    'connector' = 'kafka',
        |    'topic' = 'sink1',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |insert into sink
        |select id, a.goods_id, b.goods_name, a.goods_count, a.price_total
        |from source as a
        |join dim_goods FOR SYSTEM_TIME AS OF a.proctime as b
        |on a.goods_id = b.goods_id
        |;
        |
        |""".stripMargin

    /// kafka 开窗 -单流开窗统计
    val sql4 =
      """
        |-- set pipeline.operator-chaining = false;
        |
        |create table source (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价',
        |    proctime as proctime()
        |) with (
        |    'connector' = 'kafka',
        |    'topic' = 'source1',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'scan.startup.mode' = 'latest-offset',
        |    'properties.group.id' = 'test',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |create table dim_goods (
        |    goods_id integer comment '商品id',
        |    goods_name string comment '商品名称',
        |    dt integer comment '分区'
        |) with (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://hadoop01:3306/test',
        |    'username' = 'test',
        |    'password' = 'test',
        |    'table-name' = 'dim_goods',
        |    'lookup.cache.max-rows' = '10000',
        |    'lookup.cache.ttl' = '1 min'
        |)
        |;
        |
        |
        |create table sink (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_name string comment '商品名称',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价',
        |    window_start timestamp(3) comment '窗口开始时间',
        |    window_end timestamp(3) comment '窗口结束时间',
        |    primary key(id, goods_id, goods_name, window_start, window_end) not enforced
        |) with (
        |    'connector' = 'upsert-kafka',
        |    'topic' = 'sink1',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'key.format' = 'csv',
        |    'value.format' = 'csv',
        |    'value.csv.field-delimiter' = ' '
        |)
        |;
        |
        |create view middle as
        |select id, a.goods_id, b.goods_name, a.goods_count, a.price_total, a.proctime
        |from source as a
        |join dim_goods FOR SYSTEM_TIME AS OF a.proctime as b
        |on a.goods_id = b.goods_id and dt in (1)
        |;
        |
        |insert into sink
        |select id, goods_id, goods_name, sum(goods_count) as goods_count, sum(price_total) as price_total, window_start, window_end
        |from
        |    table(
        |        cumulate(table middle, descriptor(proctime), interval '1' minutes, interval '1' day)
        |        )
        |group by id, goods_id, goods_name, window_start, window_end
        |;
        |
        |""".stripMargin

    // 多流合并开窗统计 - 将多源合并之后开窗
    val sql5 =
      """
        |-- set pipeline.operator-chaining = false;
        |
        |create table source1 (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价',
        |    proctime as proctime()
        |) with (
        |    'connector' = 'kafka',
        |    'topic' = 'source1',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'scan.startup.mode' = 'latest-offset',
        |    'properties.group.id' = 'test',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |create table source2 (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价',
        |    proctime as proctime()
        |) with (
        |    'connector' = 'kafka',
        |    'topic' = 'source2',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'scan.startup.mode' = 'latest-offset',
        |    'properties.group.id' = 'test',
        |    'format' = 'csv',
        |    'csv.field-delimiter' = ' '
        |)
        |;
        |
        |create table dim_goods (
        |    goods_id integer comment '商品id',
        |    goods_name string comment '商品名称',
        |    dt integer comment '分区'
        |) with (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://hadoop01:3306/test',
        |    'username' = 'test',
        |    'password' = 'test',
        |    'table-name' = 'dim_goods',
        |    'lookup.cache.max-rows' = '10000',
        |    'lookup.cache.ttl' = '1 min'
        |)
        |;
        |
        |
        |create table sink (
        |    id integer comment '订单id',
        |    goods_id integer comment '商品id',
        |    goods_name string comment '商品名称',
        |    goods_count integer comment '购买商品数量',
        |    price_total double comment '总价',
        |    window_start timestamp(3) comment '窗口开始时间',
        |    window_end timestamp(3) comment '窗口结束时间',
        |    primary key(id, goods_id, goods_name, window_start, window_end) not enforced
        |) with (
        |    'connector' = 'upsert-kafka',
        |    'topic' = 'sink1',
        |    'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |    'key.format' = 'csv',
        |    'value.format' = 'csv',
        |    'value.csv.field-delimiter' = ' '
        |)
        |;
        |
        |create view middle as
        |    select id, a.goods_id, b.goods_name, a.goods_count, a.price_total, a.proctime
        |    from source1 as a
        |    join dim_goods FOR SYSTEM_TIME AS OF a.proctime as b
        |    on a.goods_id = b.goods_id and dt in (1)
        |union all
        |    select id, a.goods_id, b.goods_name, a.goods_count, a.price_total, a.proctime
        |    from source2 as a
        |    join dim_goods FOR SYSTEM_TIME AS OF a.proctime as b
        |    on a.goods_id = b.goods_id and dt in (1)
        |;
        |
        |
        |insert into sink
        |select id, goods_id, goods_name, sum(goods_count) as goods_count, sum(price_total) as price_total, window_start, window_end
        |from
        |    table(
        |        cumulate(table middle, descriptor(proctime), interval '1' minutes, interval '1' day)
        |        )
        |group by id, goods_id, goods_name, window_start, window_end
        |;
        |
        |""".stripMargin

    // WITH 子句
    val sql6 =
      """
        |create temporary table source(
        |  s1 string,
        |  s2 string
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'wzq_source',
        |  'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |  'properties.group.id' = 'test-1',
        |  'scan.startup.mode' = 'latest-offset',
        |  'scan.topic-partition-discovery.interval' = '1 h',
        |  'format' = 'csv'
        |);
        |
        |create temporary table sink(
        |  s1 string
        |) with (
        |  'connector' = 'kafka',
        |  'topic' = 'wzq_sink',
        |  'properties.bootstrap.servers' = '${kafka-bootstrapserver}',
        |  'format' = 'csv'
        |);
        |
        |insert into sink
        |with with1 as (
        |select concat(s1, '|', s2) as w1
        |from source
        |),
        |with2 as (
        |select concat(w1, '-', 'w2') as w2
        |from with1
        |)
        |select w2
        |from with2
        |;
        |""".stripMargin
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
