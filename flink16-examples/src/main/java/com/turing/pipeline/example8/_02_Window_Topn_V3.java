package com.turing.pipeline.example8;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @descri 10分钟滚动窗口内交易总额最高的前两家供应商，及其交易总额和交易单数
 *
 * @author lj.michale
 * @date 2023-03-13
 */
public class _02_Window_Topn_V3 {

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

        // 10分钟滚动窗口内交易总额最高的前两家供应商，及其交易总额和交易单数
        String executeSql = "select\n" +
                "  *\n" +
                "from(\n" +
                "  select\n" +
                "   window_start,\n" +
                "   window_end,\n" +
                "   supplier_id,\n" +
                "   sum_price,\n" +
                "   cnt,\n" +
                "   row_number() over(partition by window_start,window_end order by sum_price desc ) as rn \n" +
                "   from(\n" +
                "      select\n" +
                "        window_start,\n" +
                "        window_end,\n" +
                "        supplier_id,\n" +
                "        sum(price) as sum_price,\n" +
                "        count(1) as cnt\n" +
                "      from table(\n" +
                "        tumble(table source_table,descriptor(rt),interval '10' minute)\n" +
                "      ) group by window_start,window_end,supplier_id\n" +
                "  ) t1\n" +
                ") t1 where rn <= 2";

        tenv.executeSql(executeSql).print();

    }

}
