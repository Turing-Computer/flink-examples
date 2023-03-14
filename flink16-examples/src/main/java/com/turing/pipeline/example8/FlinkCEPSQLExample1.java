package com.turing.pipeline.example8;

import com.flink.pipeline.example8.FlinkTableStreamPipelineByStreamTableEnvironmentDemo;
import com.turing.bean.Ticker;
import com.turing.common.FlinkEnvUtils;
import com.turing.pipeline.example3.FlinkPipelineExample;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-14
 */
public class FlinkCEPSQLExample1 {

    private static final Logger logger = LoggerFactory.getLogger(FlinkCEPSQLExample1.class);

    public static void main(String[] args) {

        EnvironmentSettings settings = null;
        StreamTableEnvironment tEnv = null;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        tEnv = StreamTableEnvironment.create(env, settings);

        System.out.println("===============CEP_SQL_10=================");
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        DataStream<Ticker> dataStream =
                env.fromElements(
                        new Ticker(1, "ACME", 22, 1, LocalDateTime.parse("2021-12-10 10:00:00", dateTimeFormatter)),
                        new Ticker(3, "ACME", 19, 1, LocalDateTime.parse("2021-12-10 10:00:02", dateTimeFormatter)),
                        new Ticker(4, "ACME", 23, 3, LocalDateTime.parse("2021-12-10 10:00:03", dateTimeFormatter)),
                        new Ticker(5, "Apple", 25, 2, LocalDateTime.parse("2021-12-10 10:00:04", dateTimeFormatter)),
                        new Ticker(6, "Apple", 18, 1, LocalDateTime.parse("2021-12-10 10:00:05", dateTimeFormatter)),
                        new Ticker(7, "Apple", 16, 1, LocalDateTime.parse("2021-12-10 10:00:06", dateTimeFormatter)),
                        new Ticker(8, "Apple", 14, 2, LocalDateTime.parse("2021-12-10 10:00:07", dateTimeFormatter)),
                        new Ticker(9, "Apple", 19, 2, LocalDateTime.parse("2021-12-10 10:00:08", dateTimeFormatter)),
                        new Ticker(10, "Apple", 25, 2, LocalDateTime.parse("2021-12-10 10:00:09", dateTimeFormatter)),
                        new Ticker(11, "Apple", 11, 1, LocalDateTime.parse("2021-12-10 10:00:11", dateTimeFormatter)),
                        new Ticker(12, "Apple", 15, 1, LocalDateTime.parse("2021-12-10 10:00:12", dateTimeFormatter)),
                        new Ticker(13, "Apple", 19, 1, LocalDateTime.parse("2021-12-10 10:00:13", dateTimeFormatter)),
                        new Ticker(14, "Apple", 25, 1, LocalDateTime.parse("2021-12-10 10:00:14", dateTimeFormatter)),
                        new Ticker(15, "Apple", 19, 1, LocalDateTime.parse("2021-12-10 10:00:15", dateTimeFormatter)),
                        new Ticker(16, "Apple", 15, 1, LocalDateTime.parse("2021-12-10 10:00:16", dateTimeFormatter)),
                        new Ticker(17, "Apple", 19, 1, LocalDateTime.parse("2021-12-10 10:00:17", dateTimeFormatter)),
                        new Ticker(18, "Apple", 15, 1, LocalDateTime.parse("2021-12-10 10:00:18", dateTimeFormatter)));

        Table table = tEnv.fromDataStream(dataStream, Schema.newBuilder()
                .column("id", DataTypes.BIGINT())
                .column("symbol", DataTypes.STRING())
                .column("price", DataTypes.BIGINT())
                .column("tax", DataTypes.BIGINT())
                .column("rowtime", DataTypes.TIMESTAMP(3))
                .watermark("rowtime", "rowtime - INTERVAL '1' SECOND")
                .build());
        tEnv.createTemporaryView("CEP_SQL_10", table);

        String sql = "SELECT * " +
                "FROM CEP_SQL_10 " +
                "    MATCH_RECOGNIZE ( " +
                "        PARTITION BY symbol " +       //按symbol分区，将相同卡号的数据分到同一个计算节点上。
                "        ORDER BY rowtime " +          //在窗口内，对事件时间进行排序。
                "        MEASURES " +                   //定义如何根据匹配成功的输入事件构造输出事件
                "            e1.id as id,"+
                "            AVG(e1.price) as avgPrice,"+
                "            e1.rowtime AS start_tstamp, " +
                "            e3.rowtime AS end_tstamp " +
                "        ONE ROW PER MATCH " +                                      //匹配成功输出一条
                "        AFTER MATCH  skip to next row " +                   //匹配后跳转到下一行
                "        PATTERN ( e1 e2* e3) WITHIN INTERVAL '2' MINUTE" +
                "        DEFINE " +                                                 //定义各事件的匹配条件
                "            e1 AS " +
                "                e1.price = 25 , " +
                "            e2 AS " +
                "                e2.price > 10 AND e2.price <19," +
                "            e3 AS " +
                "                e3.price = 19 " +
                "    ) MR";


        TableResult res = tEnv.executeSql(sql);
        res.print();
        tEnv.dropTemporaryView("CEP_SQL_10");

    }

}
