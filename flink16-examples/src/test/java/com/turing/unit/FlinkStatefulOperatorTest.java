//package com.turing.unit;
//
//import com.turing.pipeline.unit.StatefulFlatMapFunction;
//import com.turing.pipeline.unit.StatefulMapFunction;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.streaming.api.operators.StreamFlatMap;
//import org.apache.flink.streaming.api.operators.StreamMap;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.util.Arrays;
//
///**
// * @descri
// *
// * @author lj.michale
// * @date 2023-03-15
// */
//public class FlinkStatefulOperatorTest {
//
//    @Test
//    public void testStatefulMapFunction() throws Exception {
//        //instantiate user-defined function
//        StatefulMapFunction statefulMapFunction = new StatefulMapFunction();
//
//        // wrap user defined function into a the corresponding operator
//        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
//                new KeyedOneInputStreamOperatorTestHarness<>(
//                        new StreamMap<>(statefulMapFunction),
//                        x -> 1,
//                        Types.INT);
//
//        // open the test harness (will also call open() on RichFunctions)
//        testHarness.open();
//
//        //push (timestamped) elements into the operator (and hence user defined function)
//        testHarness.processElement(2, 100L);
//        testHarness.processElement(3, 102L);
//
//        //retrieve list of emitted records for assertions
//        Assert.assertEquals(testHarness.extractOutputValues(), Arrays.asList(2, 5));
//    }
//
//    @Test
//    public void testStatefulFlatMapFunction() throws Exception {
//        //instantiate user-defined function
//        StatefulFlatMapFunction statefulFlatMapFunction = new StatefulFlatMapFunction();
//
//        // wrap user defined function into a the corresponding operator
//        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Integer> testHarness =
//                new KeyedOneInputStreamOperatorTestHarness<>(
//                        new StreamFlatMap<>(statefulFlatMapFunction),
//                        x -> 1,
//                        Types.INT);
//
//        // open the test harness (will also call open() on RichFunctions)
//        testHarness.open();
//
//        //push (timestamped) elements into the operator (and hence user defined function)
//        testHarness.processElement(3, 100L);
//        testHarness.processElement(9, 102L);
//
//        //retrieve list of emitted records for assertions
//        Assert.assertEquals(testHarness.extractOutputValues(), Arrays.asList(3, 12));
//    }
//}
