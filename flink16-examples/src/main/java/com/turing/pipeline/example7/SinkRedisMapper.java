package com.turing.pipeline.example7;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @descri
 *
 * @author lj.michale
 * @date 2023-03-13
 */
public class SinkRedisMapper implements RedisMapper<Tuple2<String, String>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        // hset
        return new RedisCommandDescription(RedisCommand.HSET, "flink");
    }

    @Override
    public String getKeyFromData(Tuple2<String, String> stringIntegerTuple2) {
        return stringIntegerTuple2.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, String> stringIntegerTuple2) {
        return stringIntegerTuple2.f1.toString();
    }
}