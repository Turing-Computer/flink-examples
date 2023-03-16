package com.turing.pipeline.example9;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @descri
 * 实现一个简单的周期性的发射水印的例子
 * 在这个onEvent方法里，我们从每个元素里抽取了一个时间字段，但是我们并没有生成水印发射给下游，而是自己保存了在一个变量里，在onPeriodicEmit方法里，使用最大的日志时间减去我们想要的延迟时间作为水印发射给下游。
 * @author lj.michale
 * @date 2023-03-16
 */
public class MyWaterMarks implements WatermarkGenerator<Tuple2<String,Long>>{

    private long maxTimestamp;
    // 设置允许乱序时间
    private long delay = 5000;

    /**
     * 为每个事件调用，允许水印生成器检查并记住事件时间戳，或基于事件本身发出水印。
     * */
    @Override
    public void onEvent(Tuple2<String, Long> event, long eventTimestamp, WatermarkOutput output) {
        // TODO Auto-generated method stub
        //记录最新的数据时间的值
        //maxTimestamp = Math.max(maxTimestamp, event.f1);
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        System.err.println("maxTimestamp: " + maxTimestamp + " eventTimestamp: " + eventTimestamp);
    }

    /**
     * 定期调用，并且可能会发出新的水印。
     * */
    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // TODO Auto-generated method stub
        //返回实际可接纳的时间，保障已有的数据时间 《= 水印
        output.emitWatermark(new Watermark(maxTimestamp - delay));
        //System.err.println("水印：" +  new Watermark(maxTimestamp - delay).toString());
    }

}