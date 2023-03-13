package com.turing.pipeline.example8;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @descri 10分钟滚动窗口中的交易金额最大的前2笔订单
 *
 * @author lj.michale
 * @date 2023-03-13
 */

public class _02_Window_Topn_V2 {
    public static void main(String[] args)  {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 从kafka中读取数据
        String sourceTable = "CREATE TABLE source_table (\n" +
                "  bidtime string ,\n" +
                "  `price` double,\n" +
                "  `item` STRING,\n" +
                "  `supplier_id` STRING,\n" +
                "  `rt` as cast( bidtime as timestamp(3) ),\n" +
                "   watermark for rt as rt - interval '5' second\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topn1',\n" +
                "  'properties.bootstrap.servers' = 'hadoop01:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                ")";

        tenv.executeSql(sourceTable);
        // 10分钟滚动窗口中的交易金额最大的前2笔订单
        tenv.executeSql("select\n" +
                "  *\n" +
                "from(\n" +
                "  select window_start,window_end, \n" +
                "    bidtime,\n" +
                "    price,\n" +
                "    item,\n" +
                "    supplier_id,\n" +
                "    row_number() over(partition by window_start,window_end order by price desc ) as rn\n" +
                "  from table(\n" +
                "    tumble(table source_table,descriptor(rt),interval '10' minute)\n" +
                "  ) \n" +
                ") t1 where rn <= 2 ").print();
    }
}

//## 结果如下
//+----+-------------------------+-------------------------+-------------------------+-------+---------+--------------+-------+
//| op |            window_start |              window_end |                 bidtime | price |    item |  supplier_id |    rn |
//+----+-------------------------+-------------------------+-------------------------+-------+---------+--------------+-------+
//| +I | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:00.000 |   5.0 |       D |    supplier3 |     1 |
//| +I | 2020-04-15 08:00:00.000 | 2020-04-15 08:10:00.000 | 2020-04-15 08:09:00.000 |   5.0 |       D |    supplier2 |     2 |
