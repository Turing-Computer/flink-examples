package com.flink.pipeline.example1

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class CustomGenerator extends SourceFunction[List[Double]] {
  private var running = true

  override def run(ctx: SourceFunction.SourceContext[List[Double]]): Unit = {
    // 随机数生成器
    var randomNum: Random = new Random()

    while (running) {
      val n = 1.to(5).map(i => {
        i + randomNum.nextGaussian()
      }).toList
      // 利用ctx上下文将数据返回
      ctx.collect(n)

      Thread.sleep(500)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
