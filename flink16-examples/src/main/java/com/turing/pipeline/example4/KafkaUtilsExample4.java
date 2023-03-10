package com.turing.pipeline.example4;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.turing.common.FlinkEnvUtils;
import com.turing.common.KafkaUtils;
import com.turing.pipeline.example3.FlinkPipelineExample;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-10
 */
public class KafkaUtilsExample4 {

    private static final Logger logger = LoggerFactory.getLogger(KafkaUtilsExample4.class);

    public static void main(String[] args) throws Exception {

        final String fileName = "application-dev.properties";
        InputStream inputStream = FlinkPipelineExample.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        int parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism","1"));
        String checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url");
        logger.info("flink.pipeline.parallelism:{}", parallelisNum);

        FlinkEnvUtils.FlinkEnv flinkEnv = FlinkEnvUtils.getStreamTableEnv(args);
        flinkEnv.env().setParallelism(parallelisNum);
        // 状态后端使用RocksDB
        flinkEnv.env().setStateBackend(new EmbeddedRocksDBStateBackend());
        flinkEnv.env().getCheckpointConfig().setCheckpointStorage(checkpointPath);

        // 以下是kafka的offset设置
        // OffsetsInitializer.committedOffsets()
        // OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST)
        // OffsetsInitializer.timestamp(1592323200L)
        // OffsetsInitializer.earliest()
        // OffsetsInitializer.latest()
        KafkaUtils.getNewKafkaSource(flinkEnv.env(), "turing-massage-test", "turing1", OffsetsInitializer.latest())
                .map(new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSON.parseObject(value,JSONObject.class);
                    }
                }).print("测试");

        flinkEnv.env().execute("KafkaUtilsExample4");
    }

}
