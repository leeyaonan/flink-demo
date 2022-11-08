package com.leeyaonan.chapter05.sink;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 输出算子-输出到ES
 */
public class SinkToESTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        // httpHostList
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost", 9200));

        // 定义ElasticsearchSinkFunction
        ElasticsearchSinkFunction<Event> elasticsearchSinkFunction = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> map = new HashMap<>();
                map.put(event.user, event.url);

                // 构建一个IndexRequest
                IndexRequest request = Requests.indexRequest().index("clicks").source(map);

                requestIndexer.add(request);
            }
        };

        // 写入Redis
        stream.addSink(new ElasticsearchSink.Builder<Event>(httpHosts, elasticsearchSinkFunction).build());

        env.execute();
    }

}
