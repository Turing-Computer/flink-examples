package com.turing.pipeline.example9;


import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @descri 自定义水印搭配滚动时间窗口效果
 *
 * @author lj.michale
 * @date 2023-03-16
 */
public class WaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置水印生成时间间隔100ms
        env.getConfig().setAutoWatermarkInterval(100);
        //设置Flink系统使用时间为事件时间EventTime , 即时间特征
        //env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        //设置一个延迟6秒的固定水印
        DataStream<Tuple2<String,Long>> source = env.addSource(new WindowImageSource());
        source.print("in ");

        /*env.socketTextStream("localhost", 9999);*/
        SingleOutputStreamOperator<Tuple2<String,Long>> dataStream = source
                .map(data -> new Tuple2<String,Long>(
                data.f0,data.f1
                // data.split(",")[0],  Long.parseLong(data.split(",")[1].trim())
                // 使用Lambda表达式返回必须指定返回类型
        )).returns(Types.TUPLE(Types.STRING, Types.LONG));

        dataStream.assignTimestampsAndWatermarks(
                //WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(6))
                //WatermarkStrategy.forMonotonousTimestamps()
                //WatermarkStrategy.forGenerator(new MyWaterMarks);
                new WatermarkStrategy<Tuple2<String,Long>>(){
                    private static final long serialVersionUID = 1L;
                    @Override
                    public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        // TODO Auto-generated method stub
                        return new MyWaterMarks();
                    }
                }
        ).keyBy((event) -> event.f0)
                //.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .window(TumblingEventTimeWindows.of(Time.seconds(6)))
                //.max(1)
                .process(new ProcessWindowFunction<Tuple2<String,Long>, String, String, TimeWindow>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public void process(String key, Context context,
                                        Iterable<Tuple2<String, Long>> input,
                                        Collector<String> out) {
                        long count = 0;
                        //集合
                        ArrayList<Long> conllect = new ArrayList<Long>();
                        for (Tuple2<String, Long> in: input) {
                            conllect.add(in.f1);
                            count++;
                        }

                        out.collect("Window: " + context.window() + "count: " + count + " 数据：" + input.toString());
                    }
                }).printToErr("out ");

        env.execute("Cbry WaterMark Test");
    }
}

