package com.turing.pipeline.unit;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-15
 */
public class IncrementMapFunction implements MapFunction<Integer, Integer> {
    @Override
    public Integer map(Integer value) throws Exception {
        return value + 1;
    }
}
