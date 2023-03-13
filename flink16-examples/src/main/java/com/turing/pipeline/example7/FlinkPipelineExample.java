package com.turing.pipeline.example7;

import com.turing.common.FlinkEnvUtils;
import com.turing.function.LineSplitter;
import com.turing.pipeline.example4.KafkaUtilsExample4;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @descri 连接kafka并把内容sink到redis
 *
 * @author lj.michale
 * @date 2023-03-13
 */
public class FlinkPipelineExample {

    private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineExample.class);

    // 定义函数逻辑
    public static class SubstringFunction extends ScalarFunction {
        public String eval(String s, Integer begin, Integer end) {
            return s.substring(begin, end);
        }
    }

    public static void main(String[] args) throws Exception {

        final String fileName = "application-dev.properties";
        InputStream inputStream = com.turing.pipeline.example3.FlinkPipelineExample.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        int parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism","1"));
        String checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url");
        logger.info("flink.pipeline.parallelism:{}", parallelisNum);

        FlinkEnvUtils.FlinkEnv flinkEnv = FlinkEnvUtils.getStreamTableEnv(args);
        flinkEnv.env().setParallelism(parallelisNum);
        flinkEnv.env().setStateBackend(new EmbeddedRocksDBStateBackend());
        flinkEnv.env().getCheckpointConfig().setCheckpointStorage(checkpointPath);

        KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers("localhost:9092")
                .setTopics("test").setGroupId("test01").setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema()).build();
        DataStream<String> testDataStreamSource =
                flinkEnv.env().fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<Tuple2<String, String>> data = testDataStreamSource.flatMap(new LineSplitter());
        FlinkJedisPoolConfig conf =
                new FlinkJedisPoolConfig.Builder().setHost("192.168.10.102").setPort(6379).setPassword("111111").build();
        data.addSink(new RedisSink<>(conf, new SinkRedisMapper()));

        /////////////////////////// Flink SQL 自定义函数使用
//        TableEnvironment tEnv = FlinkEnvUtils.getStreamTableEnv(args).streamTEnv();
//        // 在 Table API 里不经注册直接“内联”调用函数
//        tEnv.from("MyTable").select(call(SubstringFunction.class, $("myField"), 5, 12));
//        // 注册函数
//        tEnv.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);
//        // 在 Table API 里调用注册好的函数
//        tEnv.from("MyTable").select(call("SubstringFunction", $("myField"), 5, 12));
//        // 在 SQL 里调用注册好的函数
//        tEnv.sqlQuery("SELECT SubstringFunction(myField, 5, 12) FROM MyTable");

        flinkEnv.env().execute("FlinkPipelineExample");

    }
}
