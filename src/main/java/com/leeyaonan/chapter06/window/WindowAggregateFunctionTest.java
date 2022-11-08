package com.leeyaonan.chapter06.window;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;

/**
 * @author: leeyaonan
 * @date: 2022-11-03 10:26
 * @desc:
 */
public class WindowAggregateFunctionTest {

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

        stream.keyBy(data -> data.user)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>, String>() {

                            @Override
                            public Tuple2<Long, Integer> createAccumulator() {
                                return Tuple2.of(0L, 0);
                            }

                            @Override
                            public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> accumulator) {
                                return Tuple2.of(accumulator.f0 + event.timestamp, accumulator.f1 + 1);
                            }

                            @Override
                            public String getResult(Tuple2<Long, Integer> tuple2) {
                                return new Timestamp(tuple2.f0 / tuple2.f1).toString();
                            }

                            /**
                             * 一般只有会话窗口需要合并
                             */
                            @Override
                            public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> a, Tuple2<Long, Integer> b) {
                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                            }
                        })
                                .print();

        env.execute();
    }

}
