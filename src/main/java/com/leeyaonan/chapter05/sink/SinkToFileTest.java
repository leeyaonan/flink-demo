package com.leeyaonan.chapter05.sink;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 输出算子-输出到文件
 */
public class SinkToFileTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据（有界流）
        DataStreamSource<Event> stream = env.fromCollection(Event.createTestEventList(100));

        StreamingFileSink<String> streamingFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024) // 单个文件的最大字节数
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 输出间隔
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 结束条件，闲置多久结束
                                .build())
                .build();

        stream.map(data -> data.toString()).addSink(streamingFileSink);

        env.execute();
    }
}
