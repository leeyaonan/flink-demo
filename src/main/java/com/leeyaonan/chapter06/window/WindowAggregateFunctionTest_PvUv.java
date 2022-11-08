package com.leeyaonan.chapter06.window;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashSet;

/**
 * @author: leeyaonan
 * @date: 2022-11-03 10:26
 * @desc: 统计网站的PV、UV，两者相除计算用户活跃度
 */
public class WindowAggregateFunctionTest_PvUv {

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

        // 所有数据放在一起统计pv和uv
        stream.keyBy(data -> true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new AvgPv())
                .print();

        stream.print();

        env.execute();
    }

    // 自定义一个AggregateFunction，用Long保存pv个数，用HashSet做uv去重
    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> accumulator) {
            // 每来一条数据，pv个数加1，将user放入HashSet中去重
            accumulator.f1.add(event.user);
            return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
            // 窗口触发时，输出pv和uv的比值
            return (double) (accumulator.f0 / accumulator.f1.size());
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }

}
