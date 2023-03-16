package com.turing.pipeline.example9;

import java.util.Random;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-16
 */
public class WindowImageSource implements SourceFunction<Tuple2<String, Long>>{

    private static final long serialVersionUID = 1L;
    private boolean is_Running = true;

    @Override
    public void run(SourceContext<Tuple2<String, Long>> ctx) throws Exception {

        // TODO Auto-generated method stub
        Random random = new Random();
        int i = 1;

        while(is_Running) {

            Tuple2<String, Long> element = new Tuple2<String, Long>("oneToic",(long)i);
            //ctx.collect(element);
            //生成水印
            if(i % 6 ==0) {
                ctx.collectWithTimestamp(element, (long)i*1000+2617160286000L);
            } else {
                ctx.collectWithTimestamp(element, (long)i*1000+1617160286000L);
            }
            i++;
            //每1秒一个数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        // TODO Auto-generated method stub
        is_Running=false;
    }
}
