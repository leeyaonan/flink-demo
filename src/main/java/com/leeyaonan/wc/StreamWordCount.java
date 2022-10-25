package com.leeyaonan.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author: leeyaonan
 * @date: 2022-10-19 17:04
 * @desc: 无界流处理-wordCount
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        // 1. 创建流式的处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 读取文本流
        DataStreamSource<String> lineDataStreamSource = env.socketTextStream("localhost", 8888);

        // 3. 转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 按照word进行分组
//        KeyedStream<Tuple2<String, Long>, Tuple> wordAndOneDataStream = wordAndOneTuple.keyBy(0); // 已弃用，不建议使用
        KeyedStream<Tuple2<String, Long>, String> wordAndOneDataStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneDataStream.sum(1);

        // 6. 打印结果
        sum.print();

        // 7. 启动执行（流处理的数据因为是动态的，所以需要一个启动的操作，启动之后不断的读取数据，刷新结果）
        env.execute();

        /*
        output:
        ---------------
        1> (java,1)
        2> (hello,1)
        3> (world,1)
        2> (hello,2)
        2> (hello,3)
        4> (flink,1)
        ---------------
         */
    }
}
