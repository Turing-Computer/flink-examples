package com.flink.pipeline.example3

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner

class MyPartitioner extends FlinkKafkaPartitioner[(String, String, String)] {
  override def partition(record: (String, String, String), key: Array[Byte], value: Array[Byte], targetTopic: String, partitions: Array[Int]): Int = {
    // record: 来源kafka的topic、key、value
    Math.abs(new String(record._2).hashCode % partitions.length)
  }
}