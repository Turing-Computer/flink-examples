package com.flink.pipeline.example3

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema
import org.apache.kafka.clients.consumer.ConsumerRecord

import java.nio.charset.StandardCharsets

class MyKafkaDeserializationSchemaTuple3 extends KafkaDeserializationSchema[(String, String, String)] {

  override def deserialize(record: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String, String) = {
    new Tuple3[String, String, String](
      record.topic(),
      new String(record.key(), StandardCharsets.UTF_8),
      new String(record.value(), StandardCharsets.UTF_8))
  }

  override def isEndOfStream(nextElement: (String, String, String)): Boolean = false

  override def getProducedType: TypeInformation[(String, String, String)] = {
    TypeInformation.of(classOf[(String, String, String)])
  }
}