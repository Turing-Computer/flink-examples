package com.flink.pipeline.example3

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.connector.base.DeliveryGuarantee
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.kafka.clients.consumer.OffsetResetStrategy

import java.util.Properties
import scala.collection.JavaConverters._


object Main {

  def main(args: Array[String]): Unit = {
    /**
     * env
     */
    // stream环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * source
     */
    // 定义 KafkaSource
    lazy val kafkaSource: KafkaSource[(String, String, String)] = KafkaSource.builder()
      // Kafka消费者的各种配置文件，此处省略配置
//      .setProperties(new Properties())
      .setBootstrapServers("192.168.10.102:9092")
      // 配置消费的一个或多个topic
      .setTopics("turing-massage-test,sourceTopic2,...".split(",", -1).toList.asJava)
      .setGroupId("turing1")
      // 开始消费位置，从已提交的offset开始消费，没有的话从最新的消息开始消费
      .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
      // 反序列化，使用之前我们自定义的反序列化器
      .setDeserializer(KafkaRecordDeserializationSchema.of(new MyKafkaDeserializationSchemaTuple3))
      .build()
    // 添加 kafka source
    val inputDS: DataStream[(String, String, String)] = env.fromSource(
      kafkaSource,
      WatermarkStrategy.noWatermarks(),
      "MyKafkaSource")
      .setParallelism(1)

    /**
     * transformation
     */
    // 数据加工处理，此处省略

    /**
     * sink
     */
    val sinkProps = new Properties()
    sinkProps.setProperty("", "")
    // 定义 KafkaSink
    lazy val kafkaSink: KafkaSink[(String, String, String)] =
    KafkaSink.builder[(String, String, String)]()
      // 目标集群地址
      .setBootstrapServers("192.168.10.102:9092")
      // Kafka生产者的各种配置文件，此处省略配置
//      .setKafkaProducerConfig(new Properties())
      // 定义消息的序列化模式
      .setRecordSerializer(KafkaRecordSerializationSchema.builder()
        // Topic选择器，使用之前我们自定义的Topic选择器
        .setTopicSelector(new MyTopicSelector)
        // Key的序列化器，使用之前我们自定义的Key序列化器
        .setKeySerializationSchema(new MyKeySerializationSchema)
        // Value的序列化器，使用之前我们自定义的Value序列化器
        .setValueSerializationSchema(new MyValueSerializationSchema)
        // 自定义分区器，使用之前我们自定义的自定义分区器
        .setPartitioner(new MyPartitioner)
        .build()
      )
      // 语义保证，保证至少一次
      .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
      .build()

    // 添加 kafka sink
    inputDS.sinkTo(kafkaSink)
      .name("MyKafkaSink")
      .setParallelism(1)

    /**
     * execute
     */
    env.execute("myJob")

  }

}