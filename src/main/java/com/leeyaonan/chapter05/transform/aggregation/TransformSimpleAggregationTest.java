package com.leeyaonan.chapter05.transform.aggregation;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 简单聚合
 */
public class TransformSimpleAggregationTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据（有界流）
        DataStreamSource<Event> stream = env.fromCollection(Event.createTestEventList(100));

        // 按键分区：查询用户最新访问的记录
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.user;
            }
        }).max("timestamp").print("max: ");

        stream.keyBy(data -> data.user)
                .maxBy("timestamp")
                .print("max2: ");

        env.execute();
    }
}
