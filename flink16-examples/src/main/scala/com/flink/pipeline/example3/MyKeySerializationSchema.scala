package com.flink.pipeline.example3

import org.apache.flink.api.common.serialization.SerializationSchema

class MyKeySerializationSchema extends SerializationSchema[(String, String, String)] {
  override def serialize(element: (String, String, String)): Array[Byte] = {
    // element: 来源kafka的topic、key、value
    element._2.getBytes()
  }
}