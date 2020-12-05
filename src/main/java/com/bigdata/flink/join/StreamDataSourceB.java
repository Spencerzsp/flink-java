package com.bigdata.flink.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/3 13:10
 */
public class StreamDataSourceB extends RichParallelSourceFunction<Tuple3<String, String, Long>>{
    private volatile boolean flag = true;
    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        Tuple3[] tuple3s = new Tuple3[]{
                Tuple3.of("a", "shanghai", 1000000059000L), // [50000,60000)
                Tuple3.of("b", "chengdu", 1000000105000L)  // [100000,110000)

        };
        int count = 0;
        while (flag && count < tuple3s.length) {
            ctx.collect(new Tuple3<String, String, Long>(
                    tuple3s[count].f0.toString(),
                    tuple3s[count].f1.toString(),
                    Long.parseLong(tuple3s[count].f2.toString())));
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

    }
}
