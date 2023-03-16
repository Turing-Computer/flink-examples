package com.turing.pipeline.example9;

import com.turing.bean.ClientLogSource;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-16
 */
public class TimeLagWatermarkGenerator implements WatermarkGenerator<ClientLogSource> {

    private final long maxTimeLag = 5000;

    @Override
    public void onEvent(ClientLogSource event, long eventTimestamp, WatermarkOutput output) {
        // 什么也不做，我们专注于周期生产水印
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(System.currentTimeMillis() - maxTimeLag));
    }
}
