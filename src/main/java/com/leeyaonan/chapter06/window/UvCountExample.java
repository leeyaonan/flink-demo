package com.leeyaonan.chapter06.window;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * @author: leeyaonan
 * @date: 2022-11-03 10:26
 * @desc: 将AggregateFunction和ProcessWindowFunction结合起来
 */
public class UvCountExample {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        // 使用AggregateFunction和ProcessWindowFunction结合实现
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UvAgg(), new UvCountByWindow())
                .print();


        env.execute();
    }

    // 自定义实现AggregateFunction，增量聚合计算uv值
    public static class UvAgg implements AggregateFunction<Event, HashSet<String>, Long> {
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> accumulator) {
            accumulator.add(event.user);
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }




    // 实现自定义的ProcessWindowFunction，包装窗口信息输出
    public static class UvCountByWindow extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            long uv = iterable.iterator().next();

            // 结合窗口信息输出
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口 " + new Timestamp(start) + "~" + new Timestamp(end)
                    + "UV值为:" + uv);
        }
    }

}
