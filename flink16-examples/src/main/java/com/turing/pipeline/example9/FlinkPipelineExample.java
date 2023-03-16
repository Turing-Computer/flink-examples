package com.turing.pipeline.example9;

import com.turing.bean.ClientLogSource;
import com.turing.bean.Message01;
import com.turing.common.FlinkEnvUtils;
import com.turing.common.seserialize.MassageKafkaDeserialization;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Date;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-16
 */
public class FlinkPipelineExample {

    private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineExample.class);

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

        KafkaSource<Message01> source = KafkaSource.<Message01>builder()
                .setBootstrapServers("192.168.10.102:9092")
                .setTopics("turing-massage-test")
                .setGroupId("turing1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.of(new MassageKafkaDeserialization(true, true)))
                .build();

        DataStream<Message01> testDataStreamSource = flinkEnv.env().fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStreamSource<ClientLogSource> dataStreamSource = flinkEnv.env().addSource(new UserDefinedSource());

        testDataStreamSource.print();
        dataStreamSource.print();

        flinkEnv.env().execute("FlinkPipelineExample");

    }

    private static class UserDefinedSource implements SourceFunction<ClientLogSource> {
        private volatile boolean isCancel;

        @Override
        public void run(SourceContext<ClientLogSource> sourceContext) throws Exception {
            while (!this.isCancel) {
                sourceContext.collect(
                        ClientLogSource
                                .builder()
                                .id(RandomUtils.nextInt(0, 10))
                                .price(RandomUtils.nextInt(0, 100))
                                .timestamp(System.currentTimeMillis())
                                .date(new Date().toString())
                                .build()
                );

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            this.isCancel = true;
        }

    }

}
