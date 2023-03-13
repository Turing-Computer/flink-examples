package com.turing.pipeline.example8;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @descri 各种窗口的join代码示例
 *
 * @author lj.michale
 * @date 2023-03-13
 */
public class _03_Join {
    public static void main(String[] args) {
        // 创建表的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        /**
         * 1,a,1000
         * 2,b,2000
         * 3,c,2500
         * 4,d,3000
         * 5,e,12000
         */
        // 从socket流中读取数据
        DataStreamSource<String> s1 = env.socketTextStream("hadoop01", 9999);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss1 = s1.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String line) throws Exception {
                String[] arr = line.split(",");
                return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
            }
        });

        /**
         * 1,bj,1000
         * 2,sh,2000
         * 4,xa,2600
         * 5,yn,12000
         */
        DataStreamSource<String> s2 = env.socketTextStream("hadoop01", 9998);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> ss2 = s2.map(new MapFunction<String, Tuple3<String, String, Long>>() {
            @Override
            public Tuple3<String, String, Long> map(String line) throws Exception {
                String[] arr = line.split(",");
                return Tuple3.of(arr[0], arr[1], Long.parseLong(arr[2]));
            }
        });

        // 创建两个表
        tenv.createTemporaryView("t_left",ss1,
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BIGINT())
                        .columnByExpression("rt"," to_timestamp_ltz(f2,3) ")
                        .watermark("rt","rt - interval '0' second ")
                        .build()
        );

        tenv.createTemporaryView("t_right",ss2,
                Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .column("f1", DataTypes.STRING())
                        .column("f2", DataTypes.BIGINT())
                        .columnByExpression("rt"," to_timestamp_ltz(f2,3) ") // 指定事件时间
                        .watermark("rt","rt - interval '0' second ") // 指定水位线
                        .build()
        );

        // 各种窗口join的示例
        // INNER JOIN
        String innerJoinSql = "select\n" +
                "  a.f0,\n" +
                "  a.f1,\n" +
                "  a.f2,\n" +
                "  b.f0,\n" +
                "  b.f1\n" +
                "from\n" +
                "( select * from table( tumble(table t_left,descriptor(rt), interval '10' second) )  ) a\n" +
                "join\n" +
                "( select * from table( tumble(table t_right,descriptor(rt), interval '10' second) )  ) b\n" +
                // 带条件的join，必须包含2个输入表的window_start 和 window_end 等值条件
                "on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0";

//        tenv.executeSql(innerJoinSql).print();
        // left / right / full outer
        String fullJoinSql = "select\n" +
                "  a.f0,\n" +
                "  a.f1,\n" +
                "  a.f2,\n" +
                "  b.f0,\n" +
                "  b.f1\n" +
                "from\n" +
                "( select * from table( tumble(table t_left,descriptor(rt), interval '10' second) )  ) a\n" +
                "full join\n" +
                "( select * from table( tumble(table t_right,descriptor(rt), interval '10' second) )  ) b\n" +
                // 带条件的join，必须包含2个输入表的window_start 和 window_end 等值条件
                "on a.window_start = b.window_start and a.window_end = b.window_end and a.f0 = b.f0";

//        tenv.executeSql(fullJoinSql).print();

        // semi ==> where ... in ...
        String semiJoinSql = "select\n" +
                "  a.f0,\n" +
                "  a.f1,\n" +
                "  a.f2,\n" +
                "from\n" +
                "-- 1、在TVF上使用join\n" +
                "-- 2、参与join 的两个表都需要定义时间窗口\n" +
                "( select * from table( tumble(table t_left,decriptor(rt), interval '10' second) ) ) a\n" +
                "where f0 in\n" +
                "(\n" +
                "  select\n" +
                "    f0\n" +
                "  from\n" +
                "  ( select * from table( tumble(table t_right,decriptor(rt), interval '10' second) ) ) b\n" +
                "  -- 3、join 的条件中必须包含两表的window_start和 window_end的等值条件\n" +
                "  where a.window_start = b.window_start and a.window_end = b.window_end\n" +
                ")";

        //        tenv.executeSql(semiJoinSql).print();
    }
}
