package com.turing.pipeline.example3;

import com.turing.bean.Message01;
import com.turing.common.FlinkEnvUtils;
import com.turing.common.seserialize.MassageKafkaDeserialization;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-10
 */
public class MainAppPvUv {

    private static final Logger logger = LoggerFactory.getLogger(MainAppPvUv.class);

    public static void main(String[] args) throws Exception {

        final String fileName = "application-dev.properties";
        InputStream inputStream = FlinkPipelineExample.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        int parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism","1"));
        String checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url");
        logger.info("flink.pipeline.parallelism:{}", parallelisNum);

        FlinkEnvUtils.FlinkEnv flinkEnv = FlinkEnvUtils.getStreamTableEnv(args);
        flinkEnv.env().setParallelism(parallelisNum);
        flinkEnv.env().setStateBackend(new EmbeddedRocksDBStateBackend());
        flinkEnv.env().getCheckpointConfig().setCheckpointStorage(checkpointPath);

        /**
         * 因此以下使用OffsetsInitializer.latest(),这样只消息最近一条消息不会每次启动进行全量刷新
         */
        KafkaSource<Message01> source = KafkaSource.<Message01>builder()
                .setBootstrapServers("192.168.10.102:9092")
                .setTopics("turing-massage-test")
                .setGroupId("turing1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new MassageKafkaDeserialization(true, true)))
                .build();

        DataStream<Message01> testDataStreamSource = flinkEnv.env()
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        testDataStreamSource.print();

        flinkEnv.env().execute("FlinkPipelineExample");

    }
}
