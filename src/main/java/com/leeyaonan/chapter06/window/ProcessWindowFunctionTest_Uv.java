package com.leeyaonan.chapter06.window;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;

/**
 * @author: leeyaonan
 * @date: 2022-11-03 10:26
 * @desc: 全窗口函数：ProcessWindowFunction计算UV
 */
public class ProcessWindowFunctionTest_Uv {

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

        // 所有数据放在一起统计uv
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                        .print();


        env.execute();
    }

    // 实现自定义的ProcessWindowFunction，输出一条统计信息
    public static class UvCountByWindow extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            // 用一个HashSet保存user
            HashSet<String> userSet = new HashSet<>();

            // 遍历数据，放到set里去重
            for (Event event : iterable) {
                userSet.add(event.user);
            }

            long uv = userSet.size();

            // 结合窗口信息输出
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口 " + new Timestamp(start) + "~" + new Timestamp(end)
            + "UV值为:" + uv);
        }
    }

}
