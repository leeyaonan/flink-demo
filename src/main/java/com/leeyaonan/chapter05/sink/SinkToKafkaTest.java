package com.leeyaonan.chapter05.sink;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 14:31
 * @desc: Flink输出到Kafka
 */
public class SinkToKafkaTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 连接Kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));

        // 2. 用flink进行转换处理
        SingleOutputStreamOperator<String> result = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] fields = s.split(",");
                return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2].trim())).toString();
            }
        });

        // 3. 结果直接写入Kafka
        result.addSink(new FlinkKafkaProducer<String>("localhost:7777", "events", new SimpleStringSchema()));

        env.execute();
    }
}
