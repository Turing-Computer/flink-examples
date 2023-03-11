package com.turing.pipeline.example5;

import com.turing.common.FlinkEnvUtils;
import com.turing.pipeline.example4.KafkaUtilsExample4;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-10
 */
public class FlinkPipelineExample1 {

    private static final Logger logger = LoggerFactory.getLogger(KafkaUtilsExample4.class);

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

        String createTableSql = "CREATE TABLE spells_cast (\n" +
                "    wizard STRING,\n" +
                "    spell  STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'faker',\n" +
                "  'fields.wizard.expression' = '#{harry_potter.characters}',\n" +
                "  'fields.spell.expression' = '#{harry_potter.spells}'\n" +
                ");\n";

        String querySql = "SELECT wizard, spell, times_cast\n" +
                "FROM (\n" +
                "    SELECT *,\n" +
                "    ROW_NUMBER() OVER (PARTITION BY wizard ORDER BY times_cast DESC) AS row_num\n" +
                "    FROM (SELECT wizard, spell, COUNT(*) AS times_cast FROM spells_cast GROUP BY wizard, spell)\n" +
                ")\n" +
                "WHERE row_num <= 2;\n";

        flinkEnv.streamTEnv().executeSql(createTableSql);
        Table resultTable = flinkEnv.streamTEnv().sqlQuery(querySql);

        flinkEnv.streamTEnv()
                .toDataStream(resultTable, Row.class).print();

        flinkEnv.env().execute("KafkaUtilsExample4");
    }
}
