package com.turing.pipeline.example2;

import com.turing.bean.ClientLogSource;
import com.turing.common.FlinkEnvUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
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
 * @date 2023-03-10
 */
public class FlinkPipelineExample1 {

    private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineExample1.class);

    public static void main(String[] args) throws Exception {

        final String fileName = "application-dev.properties";
        InputStream inputStream = FlinkPipelineExample1.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        int parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism","1"));
        String checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url");
        logger.info("flink.pipeline.parallelism:{}", parallelisNum);

        FlinkEnvUtils.FlinkEnv flinkEnv = FlinkEnvUtils.getStreamTableEnv(args);
        flinkEnv.env().setParallelism(parallelisNum);
        // 状态后端使用RocksDB
        flinkEnv.env().setStateBackend(new EmbeddedRocksDBStateBackend());
        flinkEnv.env().getCheckpointConfig().setCheckpointStorage(checkpointPath);

        DataStreamSource<ClientLogSource> dataStreamSource = flinkEnv.env().addSource(new UserDefinedSource());
        dataStreamSource.print();

        flinkEnv.env().execute("FlinkPipelineExample1");

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
