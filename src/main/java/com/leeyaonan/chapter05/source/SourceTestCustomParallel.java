package com.leeyaonan.chapter05.source;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 14:31
 * @desc: 自定义实现Source
 */
public class SourceTestCustomParallel {

    /**
     * 并行
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Event> customStream = env.addSource(new ParallelClickSource());

        customStream.print();

        env.execute();
    }

}
