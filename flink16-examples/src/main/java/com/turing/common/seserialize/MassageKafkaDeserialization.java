package com.turing.common.seserialize;

import com.alibaba.fastjson2.JSON;
import com.turing.bean.Message01;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @descri  自定义序列化类
 *
 * @author lj.michale
 * @date 2023-03-10
 */
public class MassageKafkaDeserialization implements KafkaDeserializationSchema<Message01> {

    private static final Logger logger = LoggerFactory.getLogger(MassageKafkaDeserialization.class);
    private final String encoding = "UTF8";
    private boolean includeTopic;
    private boolean includeTimestamp;

    public MassageKafkaDeserialization(boolean includeTopic, boolean includeTimestamp) {
        this.includeTopic = includeTopic;
        this.includeTimestamp = includeTimestamp;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {

    }

    @Override
    public boolean isEndOfStream(Message01 message01) {
        return false;
    }

    @Override
    public Message01 deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        return null;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message, Collector<Message01> out) throws Exception {
        if (message != null) {
            try {
                String value = new String(message.value(), encoding);
                Message01 message01 = JSON.parseObject(value, Message01.class);
                logger.info("序列化之前的数据:{}", JSON.toJSONString(message01));
                out.collect(message01);
            } catch (Exception e) {
                logger.error("deserialize failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    @Override
    public TypeInformation<Message01> getProducedType() {
        return TypeInformation.of(Message01.class);
    }

}
