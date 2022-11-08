package com.leeyaonan.chapter05.transform.partition;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 转换算子-Map
 */
public class TransformPartitionTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从集合读取数据（有界流）
        DataStreamSource<Event> stream = env.fromCollection(Event.createTestEventList(2));

        // shuffle分区（随机均匀分区）
//        stream.shuffle().print().setParallelism(4);

        // 轮询分区（默认）
//        stream.rebalance().print().setParallelism(4);

        // rescale重缩放分区（只在当前TaskManager内部轮询，避免网络开销）
        // 以下通过添加并行数据源，模拟将数据分发到不同的分区
//        env.addSource(new RichParallelSourceFunction<Integer>() {
//            @Override
//            public void run(SourceContext<Integer> sourceContext) throws Exception {
//                for (int i = 0; i < 10; i++) {
//                    // 将奇偶数分别发送到0号和1号任务进行分区
//                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
//                        sourceContext.collect(i);
//                    }
//                }
//            }
//            @Override
//            public void cancel() {
//
//            }
//        })
//                .setParallelism(2)
//                .rescale()
//                .print()
//                .setParallelism(4);

        // 4. 广播：分发到所有分区
        stream.broadcast().print().setParallelism(4);

        // 5. 全局分区：将全部的数据分发到一个分区
        stream.global().print().setParallelism(4);

        // 6. 自定义分区
        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).print().setParallelism(4);

        env.execute();
    }
}
