package com.flink.pipeline.example1

import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

class RandomStringSource extends RichParallelSourceFunction[String]{
  @volatile private var running = true
  //初始化资源
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //初始化资源...
  }

  //随机生成数字作为数据源
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    //获取getRuntimeContext
    val number = getRuntimeContext.getNumberOfParallelSubtasks
    val index = getRuntimeContext.getIndexOfThisSubtask

    //打印出number和index
    println("这是自定义数据源,number = " + number + " ,index= " + index)
    while(running){
      val random = scala.util.Random.nextInt(100)
      random/(index + 1) match {
        //当设置为TimeCharacteristic#EventTime时可使用collectWithTimestamp
        case 0 => ctx.collect(String.valueOf(random))
        case _ =>
      }
    }
  }
  //空实现
  override def cancel(): Unit = {
    //停止输出数据
    running = false
    //销毁资源...
  }
}