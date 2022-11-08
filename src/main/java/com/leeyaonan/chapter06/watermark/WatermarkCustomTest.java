package com.leeyaonan.chapter06.watermark;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 生成并自定义水位线
 * @other:
 */
public class WatermarkCustomTest {

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

        stream.print();

        env.execute();
    }

}
