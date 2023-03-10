package com.flink.pipeline.example8;

import com.turing.bean.SensorReading;
import com.turing.common.FlinkEnvUtils;
import com.turing.pipeline.example3.FlinkPipelineExample;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.Duration;
import java.time.ZoneId;
import static org.apache.flink.table.api.Expressions.*;

/**
 * @descri  参考资料： https://blog.csdn.net/dajiangtai007/article/details/124635547
 *  nc -lk 9999
 *  s-5,1645085900,14
 *
 * @author lj.michale
 * @date 2023-03-14
 */
public class FlinkTableStreamPipelineByStreamTableEnvironmentDemo {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTableStreamPipelineByStreamTableEnvironmentDemo.class);

    public static void main(String[] args) throws Exception {

        final String fileName = "application-dev.properties";
        InputStream inputStream = FlinkPipelineExample.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        int parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism","1"));
        String checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url");
        logger.info("flink.pipeline.parallelism:{}", parallelisNum);
        StreamExecutionEnvironment flinkEnv = FlinkEnvUtils.getStreamTableEnv(args).getStreamExecutionEnvironment();
        flinkEnv.setParallelism(parallelisNum);
        flinkEnv.setStateBackend(new EmbeddedRocksDBStateBackend());
        flinkEnv.getCheckpointConfig().setCheckpointStorage(checkpointPath);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(flinkEnv);

        // 读取数据并提取时间戳指定水印生成策略
        WatermarkStrategy<SensorReading> watermarkStrategy = WatermarkStrategy
                .<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<SensorReading>() {
                    @Override
                    public long extractTimestamp(SensorReading element, long recordTimestamp) {
                        return element.getTimestamp() * 1000;
                    }
                });

        DataStream<SensorReading> tempSensorStream = flinkEnv.socketTextStream("192.168.10.102", 9999)
                .map(event -> {
                    String[] arr = event.split(",");
                    return SensorReading
                            .builder()
                            .id(arr[0])
                            .timestamp(Long.parseLong(arr[1]))
                            .temperature(Double.parseDouble(arr[2]))
                            .build();
                }).assignTimestampsAndWatermarks(watermarkStrategy);

        tEnv.getConfig().setLocalTimeZone(ZoneId.of("Asia/Shanghai"));
        // 打印流
//        tempSensorData.print();

        // 流转换为动态表, tEnv.fromDataStream(datastream[, schema]),tEnv.createTemporaryView("new_table", datastream[, schema])等价于tEnv.createTemporaryView("new_table", tEnv.fromDataStream(datastream[, schema]))
        Table table = tEnv.fromDataStream(tempSensorStream, Schema.newBuilder()
                .column("id", DataTypes.STRING())
                .column("timestamp", DataTypes.BIGINT())
                .column("temperature", DataTypes.DOUBLE())
                .columnByMetadata("rowtime", DataTypes.TIMESTAMP_LTZ(3))
//                .columnByExpression("new_cTime", "to_timestamp(from_unixtime(cast(timestamp as bigint) / 1000, 'yyyy-MM-dd HH:mm:ss'))")
                .watermark("rowtime", "rowtime - INTERVAL '1' SECOND")
                .build());
//        table.execute().print();

        tEnv.createTemporaryView("EventTable",tempSensorStream);
        // 自定义窗口并计算
//        Table result = table.window(Tumble
//                 // 窗口大小为2s
//                .over(lit(2).second())
//                // 按照eventTime排序
//                .on($("rowtime"))
//                // 定义一个Tumble window并指定别名w
//                .as("w"))
//                // 按照sensorID和窗口分组
//                .groupBy($("id"), $("w"))
//                // 统计每个窗口的平均气温
//                .select($("id"), $("temperature").avg().as("avgTemp"));
//        result.execute().print();

        //// 将表(Table)转换成流(DataStream)
        DataStream<Row> tempSensorStream2 = tEnv.toDataStream(table);

        tEnv.createTemporaryView("clickTable",table);
        Table aggResult = tEnv.sqlQuery("select user,COUNT(url) as cnt from clickTable group by user");
        DataStream<Row> tempSensorStream3 = tEnv.toChangelogStream(aggResult);


        flinkEnv.execute("FlinkTableStreamPipelineByStreamTableEnvironmentDemo");
    }

}
