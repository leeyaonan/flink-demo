package com.leeyaonan.chapter06;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import com.leeyaonan.chapter06.window.ProcessWindowFunctionTest_Uv;
import com.leeyaonan.chapter06.window.UrlCountExample;
import com.leeyaonan.chapter06.window.model.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;

/**
 * @author: leeyaonan
 * @date: 2022-11-08 14:22
 * @desc: 处理延迟数据
 */
public class LateDataTest {

    /**
     * 处理延迟数据的三重保障：
     * -- 1. Watermark的延迟
     * -- 2. 窗口的延迟关闭
     * -- 3. 侧输出流
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 采用nc的方式，模拟迟到数据
        SingleOutputStreamOperator<Event> stream = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {
                        String[] split = s.split(",");
                        return new Event(split[0].trim(), split[1].trim(), Long.valueOf(split[2].trim()));
                    }
                })
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 处理延迟数据的第一个保障
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.print("input");

        // 注意：这里定义的输出标签会被泛型擦除，可以使用匿名内部类的方式避免泛型擦除
        OutputTag<Event> lateTag = new OutputTag<Event>("late"){}; // 处理延迟数据的第三个保障

        SingleOutputStreamOperator<UrlViewCount> result = stream.keyBy(data -> data.url)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1)) // 窗口关闭允许延迟，注意这里是事件时间，由watermark来控制 // 处理延迟数据的第二个保障
                .sideOutputLateData(lateTag)
                .aggregate(new LateDataTest.UrlViewCountAgg(), new LateDataTest.UrlViewCountResult());

        result.print("result");

        result.getSideOutput(lateTag).print("late");

        env.execute();
    }

    // 增量聚合，来一条数据就加一
    public static class UrlViewCountAgg implements AggregateFunction<Event, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0l;
        }

        @Override
        public Long add(Event event, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }


    // 包装窗口信息输出
    public static class UrlViewCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            // 结合窗口信息输出
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long count = iterable.iterator().next();
            collector.collect(new UrlViewCount(url, count, start, end));
        }
    }
}
