package com.leeyaonan.chapter05.transform;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 转换算子-Map
 */
public class TransformMapTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据（有界流）
        DataStreamSource<Event> stream = env.fromCollection(Event.createTestEventList(100));

        // 转换计算-提取user字段
        // 1. 使用自定义类实现MapFunction接口
        SingleOutputStreamOperator<String> result = stream.map(new MyMapper());

        // 2. 使用匿名类
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.getUser();
            }
        });

        // 3. Lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.getUser());

        result.print("element");
        result2.print("element2");
        result3.print("element3");

        env.execute();
    }

    // 自定义MapFunction
    public static class MyMapper implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.getUser();
        }
    }


}
