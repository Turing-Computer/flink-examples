package com.turing.pipeline.example1;

import com.turing.bean.ClickEvent;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-02-28
 */
public class ClickSource implements SourceFunction<ClickEvent> {

    //声明一个标志位控制数据生成
    private Boolean running = true;

    @Override
    //泛型为Event
    public void run(SourceContext<ClickEvent> ctx) throws Exception {

        //随机生成数据
        Random random = new Random();
        //定义字段选取的数据集
        String[] users = {"Mary","Alice","Bob","Cary"};
        String[] urls = {"./home","./cart","./fav","./prod?id=100","/prod?id=10"};

        //一直循环生成数据
        while (running){
            String user = users[random.nextInt(users.length-1)];
            String url = users[random.nextInt(urls.length-1)];
            //系统当前事件的毫秒数
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            //collect收集Event发往下游
            ctx.collect(new ClickEvent(user,url,timestamp));

            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running =false;
    }
}
