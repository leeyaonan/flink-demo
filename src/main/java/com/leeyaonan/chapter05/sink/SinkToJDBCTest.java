package com.leeyaonan.chapter05.sink;

import com.leeyaonan.chapter05.Event;
import com.leeyaonan.chapter05.source.ClickSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 16:24
 * @desc: 输出算子-输出到MySQL
 * @other:
 */
public class SinkToJDBCTest {

    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.addSource(new ClickSource());

        stream.print();

        // 写入Redis
        stream.addSink(JdbcSink.sink(
                "insert into `leeyaonan`.`clicks` (user, url) values (?, ?)"
                , (((preparedStatement, event) -> {
                    preparedStatement.setString(1, event.user);
                    preparedStatement.setString(2, event.url);
                }))
                // 注意：按照视频中的写法执行后发现始终没有看到mysql中有数据，查询网络后增加如下配置即可，查看源码发现Flink对接MySQL执行sql时默认以5000为一批，可能是考虑到Flink和MySQL的性能差距问题吧
                , new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)
                        .build()
                , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/leeyaonan")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));

        env.execute();
    }

}
