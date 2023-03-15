package com.turing.unit;

import com.turing.pipeline.unit.IncrementFlatMapFunction;
import com.turing.pipeline.unit.IncrementMapFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-15
 */
public class FlinkFunctionUnitTest {

    @Test
    public void testIncrement() throws Exception {
        // instantiate your function
        IncrementMapFunction incrementer = new IncrementMapFunction();
        // call the methods that you have implemented
        Assert.assertEquals(3, incrementer.map(2).intValue());
    }

    // use Mock to simulate objects
    @Test
    public void testCustomFlatMapFunction() throws Exception {
        // instantiate your function
        IncrementFlatMapFunction incrementer = new IncrementFlatMapFunction();
        Collector<Integer> collector = mock(Collector.class);
        // call the methods that you have implemented
        incrementer.flatMap(2, collector);
        //verify collector was called with the right output
        Mockito.verify(collector, times(1)).collect(3);
    }

    // use ListCollector to simulate objects
    @Test
    public void testCustomFlatMapFunction2() throws Exception {
        // instantiate your function
        IncrementFlatMapFunction incrementer = new IncrementFlatMapFunction();
        List<Integer> list = new ArrayList<>();
        ListCollector<Integer> collector = new ListCollector<>(list);
        // call the methods that you have implemented
        incrementer.flatMap(2, collector);
        //verify collector was called with the right output
        Assert.assertEquals(Collections.singletonList(3), list);
    }


}
