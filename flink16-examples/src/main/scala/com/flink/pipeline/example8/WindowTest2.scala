//package com.flink.pipeline.example8
//
//import org.apache.commons.lang3.time.FastDateFormat
//import org.apache.flink.api.common.eventtime._
//import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
//import org.apache.flink.streaming.api.windowing.time.Time
//
//class RecordTimestampAssigner extends TimestampAssigner[(String, Int, String)] {
//  val fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
//
//  override def extractTimestamp(element: (String, Int, String), recordTimestamp: Long): Long = {
//
//    fdf.parse(element._3).getTime
//
//  }
//
//}
//
//class PeriodWatermarkGenerator extends WatermarkGenerator[(String, Int, String)] {
//  val fdf = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
//  var maxTimestamp: Long = _
//  val maxOutofOrderness = 0
//
//  override def onEvent(event: (String, Int, String), eventTimestamp: Long, output: WatermarkOutput): Unit = {
//
//    maxTimestamp = math.max(fdf.parse(event._3).getTime, maxTimestamp)
//
//  }
//
//  override def onPeriodicEmit(output: WatermarkOutput): Unit = {
//
//    output.emitWatermark(new Watermark(maxTimestamp - maxOutofOrderness - 1))
//  }
//}
//
//
//class MyWatermarkStrategy extends WatermarkStrategy[(String, Int, String)] {
//
//  override def createTimestampAssigner(context: TimestampAssignerSupplier.Context): TimestampAssigner[(String, Int, String)] = {
//
//    new RecordTimestampAssigner()
//  }
//
//  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[(String, Int, String)] = {
//    new PeriodWatermarkGenerator()
//
//  }
//
//}
//
//
//object WindowTest {
//
//  def main(args: Array[String]): Unit = {
//
//    val senv = StreamExecutionEnvironment.getExecutionEnvironment
//    val input = senv.fromElements(
//      ("A", 10, "2021-09-08 22:00:00"),
//      ("A", 20, "2021-09-08 23:00:00"),
//      ("A", 30, "2021-09-09 06:00:00"),
//      ("B", 100, "2021-09-08 22:00:00"),
//      ("B", 200, "2021-09-08 23:00:00"),
//      ("B", 300, "2021-09-09 06:00:00")
//    ).assignTimestampsAndWatermarks(new MyWatermarkStrategy())
//
//    val result: DataStream[(String, Int, String)] = input.keyBy(_._1)
//      .window(TumblingEventTimeWindows.of(Time.days(1L)))
//      .sum(1)
//
//    result.print("result")
//
//    senv.execute("WindowTest")
//  }
//}
//
