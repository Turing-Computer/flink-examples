package com.turing.common;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @descri flink执行环境初始化
 *
 * @author lj.michale
 * @date 2023-03-07
 */
public class FlinkEnvUtils {

    /**
     * 设置状态后端为 HashMapStateBackend
     *
     * @param env env
     */
    public static void setHashMapStateBackend(StreamExecutionEnvironment env) {
        setCheckpointConfig(env);
        HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
        env.setStateBackend(hashMapStateBackend);
    }

    /**
     * 设置状态后端为 EmbeddedRocksDBStateBackend
     *
     * @param env env
     */
    public static void setEmbeddedRocksDBStateBackend(StreamExecutionEnvironment env) {
        setCheckpointConfig(env);
        EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        // DEFAULT
        //SPINING_DISK_OPTIMIZED
        //SPINNING_DISK_OPTIMIZED_HIGH_MEM, 机械硬盘+内存模式
        //FLASH_SSD_OPTIMIZED  (有条件使用ssd的可以使用这个选项)
        embeddedRocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
        env.setStateBackend(embeddedRocksDBStateBackend);
    }

    /**
     * Checkpoint参数相关配置
     *
     * @param env env
     */
    public static void setCheckpointConfig(StreamExecutionEnvironment env) {
        // 每1000ms开始一次checkpoint，这个就是触发JM的checkpoint-Coordinator的一个事件间隔设置
        env.enableCheckpointing(1000);
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 允许两个连续的checkpoint错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        // 同一时间只允许一个checkpoint进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 使用externalized checkpoints，这样checkpoint在作业取消后仍就会被保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 不设置 StateBackend，即：读取 flink-conf.yaml 文件的配置
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // 作业失败重启设置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        // 解决Failed to set setXIncludeAware(true) for parser报错
        System.setProperty("javax.xml.parsers.DocumentBuilderFactory", "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");
        System.setProperty("javax.xml.parsers.SAXParserFactory", "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl");
    }

    /**
     * 流式：获取getStreamTableEnv
     *
     * @param args args
     */
    public static FlinkEnv getStreamTableEnv(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration configuration = Configuration.fromMap(parameterTool.toMap());
        configuration.setString("rest.flamegraph.enabled", "true");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        String stateBackend = parameterTool.get("state.backend", "rocksdb");
        // 判断状态后端模式
        if ("hashmap".equals(stateBackend)) {
            setHashMapStateBackend(env);
        } else if ("rocksdb".equals(stateBackend)) {
            setEmbeddedRocksDBStateBackend(env);
        }
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(6,
                Time.of(10L, TimeUnit.MINUTES), Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);

        EnvironmentSettings settings =
                EnvironmentSettings
                        .newInstance()
                        .inStreamingMode()
                        .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().addConfiguration(configuration);
        FlinkEnv flinkEnv =
                FlinkEnv.builder()
                        .streamExecutionEnvironment(env)
                        .streamTableEnvironment(tEnv)
                        .build();

        return flinkEnv;
    }

    /**
     * 批式：获取getBatchTableEnv
     *
     * @param args args
     */
    public static FlinkEnv getBatchTableEnv(String[] args) {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        Configuration configuration = Configuration.fromMap(parameterTool.toMap());
        configuration.setString("rest.flamegraph.enabled", "true");
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        String stateBackend = parameterTool.get("state.backend", "rocksdb");
        // 判断状态后端模式
        if ("hashmap".equals(stateBackend)) {
            setHashMapStateBackend(env);
        } else if ("rocksdb".equals(stateBackend)) {
            setEmbeddedRocksDBStateBackend(env);
        }
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.failureRateRestart(6,
                Time.of(10L, TimeUnit.MINUTES), Time.of(5L, TimeUnit.SECONDS)));
        env.getConfig().setGlobalJobParameters(parameterTool);

        EnvironmentSettings settings =
                EnvironmentSettings
                        .newInstance()
                        .inBatchMode()
                        .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        tEnv.getConfig().addConfiguration(configuration);
        FlinkEnv flinkEnv =
                FlinkEnv.builder()
                        .streamExecutionEnvironment(env)
                        .streamTableEnvironment(tEnv)
                        .build();

        return flinkEnv;
    }

    @Builder
    @Data
    public static class FlinkEnv {
        private StreamExecutionEnvironment streamExecutionEnvironment;

        /**
         * StreamTableEnvironment用于流计算场景，流计算的对象是DataStream。相比TableEnvironment，StreamTableEnvironment 提供了DataStream和Table之间相互转换的接口，
         * 如果用户的程序除了使用Table API & SQL编写外，还需要使用到DataStream API，则需要使用StreamTableEnvironment。
         */
        private StreamTableEnvironment streamTableEnvironment;

        private TableEnvironment tableEnvironment;

        public StreamTableEnvironment streamTEnv() {
            return this.streamTableEnvironment;
        }

        public TableEnvironment batchTEnv() {
            return this.tableEnvironment;
        }

        public StreamExecutionEnvironment env() {
            return this.streamExecutionEnvironment;
        }
    }


}