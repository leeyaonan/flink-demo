package com.leeyaonan.chapter05.transform.aggregation;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 规约聚合
 */
public class TransformReduceTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据（有界流）
        DataStreamSource<Event> stream = env.fromCollection(Event.createTestEventList(100));

        // 1. 统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> clickByUser = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user, 1L);
                    }
                })
                // 按user分区
                .keyBy(data -> data.f0)
                // 规约聚合
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });

        // 2. 选取当前最活跃的用户
        SingleOutputStreamOperator<Tuple2<String, Long>> result = clickByUser.keyBy(data -> "key")
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return value1.f1 > value2.f1 ? value1 : value2;
                    }
                });

        result.print();

        env.execute();
    }
}
