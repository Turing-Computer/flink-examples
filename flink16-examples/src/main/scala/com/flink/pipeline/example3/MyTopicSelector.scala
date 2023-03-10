package com.flink.pipeline.example3

import org.apache.flink.connector.kafka.sink.TopicSelector

class MyTopicSelector extends TopicSelector[(String, String, String)] {
  override def apply(t: (String, String, String)): String = {
    // t: 来源kafka的topic、key、value
    "turing-" + t._1.toUpperCase()
  }
}