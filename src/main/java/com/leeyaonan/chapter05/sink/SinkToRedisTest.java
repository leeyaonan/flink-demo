package com.leeyaonan.chapter05.sink;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 输出算子-输出到Redis
 * @other:
 * 本地运行这个程序的时候，遇到了很多报错，最终发现都是依赖问题，maven没有下载到指定的依赖以及关联的依赖导致类找不到，手动导入后运行成功
 * -- commons-pool2(2.11.1)
 * -- flink-connector-redis_2.11(1.0)
 * -- gson(2.10)
 * -- jedis(3.1.0) ps：使用最新版的jedis还会报hset方法找不到，故使用旧版本
 *
 * 程序执行成功后，连接redis，hgetall clicks查询结果
 */
public class SinkToRedisTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // 创建一个Jedis连接配置
        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("127.0.0.1")
                .setPort(6379)
                .build();

        // 写入Redis
        stream.addSink(new RedisSink<>(jedisConfig, new MyRedisMapper()));

        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(Event event) {
            return event.user;
        }

        @Override
        public String getValueFromData(Event event) {
            return event.url;
        }
    }

}
