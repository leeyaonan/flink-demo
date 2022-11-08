package com.leeyaonan.chapter06.window;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: leeyaonan
 * @date: 2022-11-03 10:26
 * @desc:
 */
public class WindowTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
                    /**
                     * 主要负责按照既定的方式，基于时间戳生成水位线。
                     * @param context
                     * @return
                     */
                    @Override
                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                        return new WatermarkGenerator<Event>() {
                            /**
                             * 每个事件（数据）到来都会调用的方法，
                             * 它的参数有当前事件、时间戳以及允许发出水位线的一个WatermarkOutput，
                             * 可以基于事件做各种操作
                             * @param event
                             * @param l
                             * @param watermarkOutput
                             */
                            @Override
                            public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {

                            }

                            /**
                             * 周期性调用的方法，可以由WatermarkOutput发出水位线。
                             * 周期时间为处理时间，可以调用环境配置的.setAutoWatermarkInterval()方法来设置，默认为200ms
                             * （200ms的默认配置可以在env中修改，env.getConfig().setAutoWatermarkInterval(100);）
                             * @param watermarkOutput
                             */
                            @Override
                            public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

                            }
                        };
                    }

                    /**
                     * 主要负责从流中数据元素的某个字段中提取时间戳，并分配给元素。
                     * 时间戳的分配是生成水位线的基础
                     * @param context
                     * @return
                     */
                    @Override
                    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
                        return WatermarkStrategy.super.createTimestampAssigner(context);
                    }
                });

        stream.keyBy(data -> data.user)
                // 窗口分配器: .window(xxx)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5))); // 滑动事件时间窗口
//                .window(TumblingEventTimeWindows.of(Time.hours(1))); // 滚动事件时间窗口
//                .window(EventTimeSessionWindows.withGap(Time.seconds(2))); // 事件事件会话窗口
//                .countWindow(10); // 计数滚动窗口
//                .countWindow(10, 2); // 计数滑动窗口
        env.execute();
    }

}
