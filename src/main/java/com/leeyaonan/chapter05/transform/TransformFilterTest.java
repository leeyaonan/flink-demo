package com.leeyaonan.chapter05.transform;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 转换算子-Filter
 */
public class TransformFilterTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据（有界流）
        DataStreamSource<Event> stream = env.fromCollection(Event.createTestEventList(100));

        // 转换计算-过滤Tom的元素
        // 1. 自定义FilterFunction类
        SingleOutputStreamOperator<Event> filter = stream.filter(new MyFilter());

        // 2. 匿名类
        SingleOutputStreamOperator<Event> filter1 = stream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                if ("Zay".equals(event.getUser())) {
                    return true;
                }
                return false;
            }
        });

        // 3. Lambda表达式
        SingleOutputStreamOperator<Event> filter2 = stream.filter(data -> "Rot".equals(data.user));

        filter.print("filter");
        filter1.print("filter2");
        filter2.print("filter3");

        env.execute();
    }

    public static class MyFilter implements FilterFunction<Event> {

        @Override
        public boolean filter(Event event) throws Exception {
            if ("Tom".equals(event.getUser())) {
                return true;
            }
            return false;
        }
    }

}
