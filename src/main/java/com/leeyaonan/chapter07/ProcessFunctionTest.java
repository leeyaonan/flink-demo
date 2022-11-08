package com.leeyaonan.chapter07;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author: leeyaonan
 * @date: 2022-11-08 15:43
 * @desc: ProcessFunction的实现逻辑
 */
public class ProcessFunctionTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                // 乱序流的watermark生成
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 处理延迟数据的第一个保障
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        stream.process(new ProcessFunction<Event, String>() {
            /**
             * 每来一个数据就执行一次，在这里面可以做各种复杂的操作
             *
             * @param event
             * @param context
             * @param collector
             * @throws Exception
             */
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                // 基础功能
                if (event.user.equals("Rot")) {
                    collector.collect(event.user + "click" + event.url);
                }

                System.out.println("timestamp:" + new Timestamp(context.timestamp()));
                System.out.println("watermark:" + new Timestamp(context.timerService().currentWatermark()));
                System.out.println("processTime:" + new Timestamp(context.timerService().currentProcessingTime()));

                // 富函数功能
                System.out.println("index:" + getRuntimeContext().getIndexOfThisSubtask());
                System.out.println("================\r\n");
            }

            @Override
            public void onTimer(long timestamp, ProcessFunction<Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println("onTimer");
                super.onTimer(timestamp, ctx, out);
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.printf("-----------open:%d-----------%n", getRuntimeContext().getIndexOfThisSubtask());
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                System.out.printf("-----------close:%d-----------%n", getRuntimeContext().getIndexOfThisSubtask());
                super.close();
            }
        })
                .print();

        env.execute();
    }
}
