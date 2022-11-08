package com.leeyaonan.chapter05.source;

import com.leeyaonan.chapter05.Event;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * @author: leeyaonan
 * @date: 2022-10-31 15:59
 * @desc:
 */
public class ParallelClickSource implements ParallelSourceFunction<Event> {

    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        while (running) {
            sourceContext.collect(Event.createRandomEvent());
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
