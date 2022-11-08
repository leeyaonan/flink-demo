package com.leeyaonan.chapter05.source;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 14:31
 * @desc:
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取数据（有界流）
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2. 从集合中读取数据（有界流）
        DataStreamSource<Integer> stream2 = env.fromCollection(createTestIntegerList(10));
        DataStreamSource<Event> stream3 = env.fromCollection(createTestEventList(10));

        // 3. 从元素读取数据（有界流）
        DataStreamSource<Object> stream4 = env.fromElements(
                Event.createRandomEvent(),
                Event.createRandomEvent(),
                Event.createRandomEvent(),
                Event.createRandomEvent(),
                Event.createRandomEvent()
        );

        // 4. 从socket文本流中读取（无界流）
        DataStreamSource<String> stream5 = env.socketTextStream("localhost", 7777);

        stream1.print("txt");
        stream2.print("number");
        stream3.print("event");
        stream4.print("element");
        stream5.print("localhost:7777");

        env.execute();
    }

    // ==================================以下为mock测试数据==================================

    private static List<Integer> createTestIntegerList(int i) {
        List<Integer> list = new ArrayList<>();
        for (int j = 0; j < i; j++) {
            list.add(new Random().nextInt());
        }
        return list;
    }

    private static List<Event> createTestEventList(int i) {
        List<Event> list = new ArrayList<>();
        for (int j = 0; j < i; j++) {
            list.add(Event.createRandomEvent());
        }
        return list;
    }
}
