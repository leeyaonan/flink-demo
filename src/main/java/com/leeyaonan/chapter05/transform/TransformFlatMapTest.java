package com.leeyaonan.chapter05.transform;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 转换算子-FlatMap
 */
public class TransformFlatMapTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据（有界流）
        DataStreamSource<Event> stream = env.fromCollection(Event.createTestEventList(100));

        // 1. 使用自定义类实现MapFunction接口
        SingleOutputStreamOperator<String> result = stream.flatMap(new MyFlatMapper());

        // 2. 使用匿名类
        SingleOutputStreamOperator<String> result2 = stream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                collector.collect(event.user);
                collector.collect(event.url);
                collector.collect(event.timestamp.toString());
            }
        });

        // 3. Lambda表达式
        SingleOutputStreamOperator<String> result3 = stream.flatMap((Event value, Collector<String> collector) -> {
            if ("Tom".equals(value.user)) {
                collector.collect(value.user);
            } else if ("Rot".equals(value.user)) {
                collector.collect(value.user);
                collector.collect(value.url);
                collector.collect(value.timestamp.toString());
            }
        })
                // 注意：上面的Collector中出现了泛型擦除，因此需要指明返回值的类型，不然会报错
                .returns(new TypeHint<String>() {});

        result.print("element");
        result2.print("element2");
        result3.print("element3");

        env.execute();
    }

    // 自定义MapFunction
    public static class MyFlatMapper implements FlatMapFunction<Event, String> {

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            collector.collect(event.user);
            collector.collect(event.url);
            collector.collect(event.timestamp.toString());
        }
    }


}
