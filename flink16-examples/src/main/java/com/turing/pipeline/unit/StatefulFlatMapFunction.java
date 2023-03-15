package com.turing.pipeline.unit;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-15
 */
public class StatefulFlatMapFunction extends RichFlatMapFunction<Integer, Integer> {

    // cumulative value
    private transient ValueState<Integer> total;

    @Override
    public void open(Configuration parameters) throws Exception {
        total = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalValue", Types.INT)
        );
    }

    @Override
    public void flatMap(Integer value, Collector out) throws Exception {
        if (null != total.value()) {
            value += total.value();
        }
        total.update(value);
        out.collect(value);
    }
}
