package com.leeyaonan.chapter07;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author: leeyaonan
 * @date: 2022-11-08 15:43
 * @desc: 实现按处理时间的定时器
 */
public class ProcessingTimeTimerTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                // 参数：key,input,output
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                        // 获取当前的时间戳（执行时间）
                        Long currTs = context.timerService().currentProcessingTime();
                        collector.collect(context.getCurrentKey() + " Current Timestamp is " + currTs);

                        // 注册一个10s的定时器
                        context.timerService().registerProcessingTimeTimer(currTs + 10 * 1000);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " Timer Ring!");
                        super.onTimer(timestamp, ctx, out);
                    }
                }).print();

        env.execute();
    }
}
