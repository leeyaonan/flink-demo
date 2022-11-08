package com.leeyaonan.chapter07;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import com.leeyaonan.chapter06.window.UrlCountExample;
import com.leeyaonan.chapter06.window.model.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @author: leeyaonan
 * @date: 2022-11-08 17:29
 * @desc: 应用案例：实时统计一段时间内的热门url，例如，统计最近10秒钟内最热门的两个url链接，并且每5秒钟更新一次
 */
public class TopNExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        }));

        // 1. 按照url分组，统计窗口内每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCountExample.UrlViewCountAgg(), new UrlCountExample.UrlViewCountResult());

        urlCountStream.print("url count");

        // 2. 对于同一窗口统计出的访问量，进行收集和排序
        urlCountStream.keyBy(data -> data.windowEnd)
                .process(new TopNProcessResult(2))
                .print();


        env.execute();
    }

    private static class TopNProcessResult extends KeyedProcessFunction<Long, UrlViewCount, String> {
        // 定义一个topN的参数
        private Integer n;

        // 定义列表的状态
        private ListState<UrlViewCount> urlViewCountListState;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        // 在环境中获取状态


        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount urlViewCount, KeyedProcessFunction<Long, UrlViewCount, String>.Context context, Collector<String> collector) throws Exception {
            // 将数据保存到状态中
            urlViewCountListState.add(urlViewCount);

            // 注册windowEnd + 1ms的定时器
            context.timerService().registerProcessingTimeTimer(context.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> topNList = new ArrayList<>();

            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                topNList.add(urlViewCount);
            }

            topNList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return (int) (o2.count - o1.count);
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("-----------------------\r\n");
            result.append("窗口结束时间: " + new Timestamp(ctx.getCurrentKey()) + "\r\n");

            // 取list前两个
            for (int i = 0; i < topNList.size(); i++) {
                if (i < n) {
                    UrlViewCount urlViewCount = topNList.get(i);
                    String log = "No." + (i + 1) + " "
                            + "url is " + urlViewCount.url + " "
                            + "count is " + urlViewCount.count
                            + "\r\n";
                    result.append(log);
                } else {
                    break;
                }
            }
            result.append("-----------------------\r\n");
            out.collect(result.toString());
        }
    }
}
