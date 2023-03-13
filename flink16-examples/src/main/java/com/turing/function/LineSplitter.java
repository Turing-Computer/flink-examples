package com.turing.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-13
 */

public class LineSplitter implements FlatMapFunction<String, Tuple2<String, String>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
        String[] tokens = s.toLowerCase().split(",");
        if (tokens != null && tokens.length > 0) {
            collector.collect(new Tuple2<String, String>(tokens[0], tokens[1]));
            //System.out.println(">>>>>>key->" + tokens[0] + " value->" + tokens[1]+"  into redis...");
        }
    }
}