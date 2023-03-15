package com.turing.pipeline.unit;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-15
 */
public class IncrementFlatMapFunction implements FlatMapFunction<Integer, Integer> {
    @Override
    public void flatMap(Integer value, Collector out) throws Exception {
        out.collect(value + 1);
    }
}
