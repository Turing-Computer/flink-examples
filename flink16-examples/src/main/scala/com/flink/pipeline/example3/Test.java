package com.flink.pipeline.example3;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import java.util.Arrays;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-10
 */
//在SourceFunction函数中，指定Timestamp和生成Watermark示例
public class Test {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Tuple3[] input = {
                Tuple3.of("user1", 1000L, 1),
                Tuple3.of("user1", 1999L, 2),
                Tuple3.of("user1", 2000L, 3),
                Tuple3.of("user1", 2100L, 4),
                Tuple3.of("user1", 2130L, 5)
        };

        //通过示例数据生成DataStream
        DataStream<Tuple3<String, Long, Integer>> source = env.addSource(
                //SourceFunction中进行时间戳分配和水位线生成
                new SourceFunction<Tuple3<String, Long, Integer>>() {
                    @Override
                    public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                        //遍历数据集
                        Arrays.asList(input).forEach(tp3 -> {
                            //指定时间戳
                            ctx.collectWithTimestamp(tp3, (long) tp3.f1);
                            System.out.println("collectWithTimestamp:"+ (long) tp3.f1);
                            //发送水位线，当前元素时间戳-1
                            ctx.emitWatermark(new Watermark((long) tp3.f1 - 1));
                            System.out.println("emitWatermark:"+ ((long) tp3.f1 - 1));
                            System.out.println("**************************************");
                        });
                        //代表结束标志
                        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
                    }
                    @Override
                    public void cancel() {}
                });
        //结果打印
        source.print();
        //执行程序
        env.execute();
    }
}
