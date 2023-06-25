package com.turing.common;

import com.alibaba.fastjson2.JSONObject;
import com.turing.common.config.KafkaConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.Properties;

/**
 * @descri 基于新版Kafka Connector的工具类
 *
 * @author lj.michale
 * @date 2023-03-10
 */
public class KafkaUtils {

    /**
     * 功能描述: <br>
     * 〈自定义build,生产kafkaSource〉
     *
     * @Param: [env, topic, groupId, offsets]
     * @Return: org.apache.flink.streaming.api.datastream.DataStreamSource<java.lang.String>
     */
    public static DataStreamSource<String> getNewKafkaSource(StreamExecutionEnvironment env,
                                                             String topic,
                                                             String groupId,
                                                             OffsetsInitializer offsets) {
        // 1.15 之后需要新方法创建kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setProperty("partition.discovery.interval.ms", "60000")
                .setBootstrapServers(KafkaConfig.bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(offsets)
                .build();

        return env.fromSource(source, WatermarkStrategy.noWatermarks(), topic);
    }


    /**
     * 功能描述: <br>
     * 〈kafkaSink,按照指定字段分组,设置压缩〉
     *
     * @Param: [topic, filed]
     * @Return: org.apache.flink.connector.kafka.sink.KafkaSink<java.lang.String>
     */
    public static KafkaSink<String> kafkaSink(String topic, String filed) {
        Properties properties = new Properties();
        properties.setProperty("compression.type", "lz4");
        properties.setProperty("compression.codec", "lz4");

        return KafkaSink.<String>builder()
                .setBootstrapServers(KafkaConfig.bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setPartitioner(new FlinkKafkaPartitioner<String>() {
                            @Override
                            public int partition(String record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                                JSONObject jsonObject = JSONObject.parseObject(record);
                                Object o = jsonObject.get(filed);
                                return Math.abs(o.hashCode() % partitions.length);
                            }
                        })
                        .build()
                )
                //setDeliverGuarantee 1.14官方文档有错误,1.15修改过来了
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setKafkaProducerConfig(properties)
                .build();
    }

}