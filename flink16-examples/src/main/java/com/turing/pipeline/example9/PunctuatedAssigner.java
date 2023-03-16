//package com.turing.pipeline.example9;
//
//import com.turing.bean.ClientLogSource;
//import org.apache.flink.api.common.eventtime.Watermark;
//import org.apache.flink.api.common.eventtime.WatermarkGenerator;
//import org.apache.flink.api.common.eventtime.WatermarkOutput;
//
///**
// * @descri
// *
// * @author lj.michale
// * @date 2023-03-16
// */
//public class PunctuatedAssigner implements WatermarkGenerator<ClientLogSource> {
//
//    @Override
//    public void onEvent(ClientLogSource event, long eventTimestamp, WatermarkOutput output) {
//        if (event.hasWatermarkMarker()) {
//            output.emitWatermark(new Watermark(event.getWatermarkTimestamp()));
//        }
//    }
//
//    @Override
//    public void onPeriodicEmit(WatermarkOutput output) {
//        // 什么也不做，我们专注于每个事件（流数据）的逻辑
//    }
//}
