package com.turing.unit;

import org.apache.flink.api.common.functions.MapFunction;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-15
 */
public class IncrementMapFunction implements MapFunction<Long, Long> {
    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}