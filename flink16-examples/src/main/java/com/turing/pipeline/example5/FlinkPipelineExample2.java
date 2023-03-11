package com.turing.pipeline.example5;

import com.turing.common.FlinkEnvUtils;
import com.turing.pipeline.example4.KafkaUtilsExample4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-11
 */
public class FlinkPipelineExample2 {

    private static final Logger logger = LoggerFactory.getLogger(FlinkPipelineExample2.class);

    public static void main(String[] args) throws Exception {

        final String fileName = "application-dev.properties";
        InputStream inputStream = com.turing.pipeline.example3.FlinkPipelineExample.class.getClassLoader().getResourceAsStream(fileName);
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(inputStream);
        int parallelisNum = Integer.valueOf(parameterTool.get("flink.pipeline.parallelism","1"));
        String checkpointPath = parameterTool.get("flink.pipeline.checkpoint.url");
        logger.info("flink.pipeline.parallelism:{}", parallelisNum);

        FlinkEnvUtils.FlinkEnv flinkEnv = FlinkEnvUtils.getStreamTableEnv(args);
        flinkEnv.env().setParallelism(parallelisNum);
        // 状态后端使用RocksDB
        flinkEnv.env().setStateBackend(new EmbeddedRocksDBStateBackend());
        flinkEnv.env().getCheckpointConfig().setCheckpointStorage(checkpointPath);

        String sql = "CREATE TEMPORARY TABLE datagen_source (\n" +
                "  `character_id` INT,\n" +
                "  `location` STRING,\n" +
                "  `proctime` AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen'\n" +
                ");\n" +
                "\n" +
                "CREATE TEMPORARY TABLE faker_dim (\n" +
                "  `character_id` INT,\n" +
                "  `name` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'faker',\n" +
                "  'fields.character_id.expression' = '#{number.numberBetween ''0'',''100''}',\n" +
                "  'fields.name.expression' = '#{harry_potter.characters}'\n" +
                ");\n" +
                "\n" +
                "CREATE TABLE sink_table (\n" +
                "    character_id INT,\n" +
                "    location STRING,\n" +
                "    name STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ");\n" +
                "INSERT INTO sink_table\n" +
                "SELECT\n" +
                "  c.character_id,\n" +
                "  l.location,\n" +
                "  c.name\n" +
                "FROM datagen_source AS l\n" +
                "JOIN faker_dim FOR SYSTEM_TIME AS OF proctime AS c\n" +
                "ON l.character_id = c.character_id;";

        /**
         * 算子 {@link org.apache.flink.streaming.api.operators.KeyedProcessOperator}
         * --  {@link org.apache.flink.table.runtime.operators.deduplicate.ProcTimeDeduplicateKeepFirstRowFunction}
         */
        for (String innerSql : sql.split(";")) {
            flinkEnv.streamTEnv().executeSql(innerSql);
        }

        flinkEnv.env().execute("FlinkPipelineExample2");
    }

}
