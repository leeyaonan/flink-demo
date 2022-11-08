package com.leeyaonan.chapter07;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author: leeyaonan
 * @date: 2022-11-08 15:43
 * @desc: 实现按事件时间的定时器
 */
public class EventTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).
                        withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.keyBy(data -> data.user)
                // 参数：key,input,output
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        // 获取当前的时间戳（事件时间）
                        Long currTs = context.timestamp();
                        collector.collect(context.getCurrentKey() + " Current Timestamp is " + currTs + ", watermark is " + context.timerService().currentWatermark());

                        // 注册一个10s的定时器（事件时间）
                        context.timerService().registerEventTimeTimer(currTs + 10 * 1000);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " Timer Ring!Trigger time is " + ctx.timestamp() + "Current watermark is " + ctx.timerService().currentWatermark());
                        super.onTimer(timestamp, ctx, out);
                    }
                }).print();

        env.execute();
    }
}
