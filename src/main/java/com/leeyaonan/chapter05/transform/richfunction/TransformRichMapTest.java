package com.leeyaonan.chapter05.transform.richfunction;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.transform.TransformMapTest;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 转换算子-Map
 */
public class TransformRichMapTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据（有界流）
        DataStreamSource<Event> stream = env.fromCollection(Event.createTestEventList(10));

        // 转换计算-提取user字段
        SingleOutputStreamOperator<String> result = stream.map(new TransformRichMapTest.MyRichMapper()).setParallelism(2);

        result.print("element");

        env.execute();
    }

    // 自定义RichMapFunction
    public static class MyRichMapper extends RichMapFunction<Event, String> {

        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open生命周期被调用 " + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close生命周期被调用 " + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
        }
    }
}
