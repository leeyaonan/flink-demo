package com.leeyaonan.chapter05.source;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 15:27
 * @desc:
 */
public class ClickSource implements SourceFunction<Event> {

    // 声明一个标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(Event.createRandomEvent());
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
