package com.turing.pipeline.unit;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-15
 */
public class StatefulMapFunction extends RichMapFunction<Integer, Integer> {
    // cumulative value
    private transient ValueState<Integer> total;

    @Override
    public void open(Configuration parameters) throws Exception {
        total = getRuntimeContext().getState(
                new ValueStateDescriptor<>("totalValue", TypeInformation.of(Integer.class))
        );
    }

    @Override
    public Integer map(Integer value) throws Exception {
        if (null != total.value()) {
            value += total.value();
        }
        total.update(value);
        return value;
    }
}
