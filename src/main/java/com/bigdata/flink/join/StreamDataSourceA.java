package com.bigdata.flink.join;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/3 11:52
 */
public class StreamDataSourceA extends RichParallelSourceFunction<Tuple3<String, String, Long>>{
    private volatile boolean flag = true;
    @Override
    public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
        Tuple3[] tuple3s = new Tuple3[]{
                Tuple3.of("a", "1", 1000000050000L), // [50000,60000)
                Tuple3.of("a", "2", 1000000054000L), // [50000,60000)
                Tuple3.of("a", "3", 1000000079900L), // [70000,80000)
                Tuple3.of("a", "4", 1000000115000L), // [110000,120000) 115000 - 5000 >= 109999
                Tuple3.of("b", "5", 1000000100000L), // [100000,110000)
                Tuple3.of("b", "6", 1000000108000L), // [100000,110000)
        };
        int count = 0;
        while (flag && count < tuple3s.length){
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
