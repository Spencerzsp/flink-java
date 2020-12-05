package com.bigdata.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @ author spencer
 * @ date 2020/5/26 15:30
 * 分组之后再进行countWindow，每一个组达到指定的条数才进行处理
 */
public class CountWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //spark,3
        //flink,2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                String word = fields[0];
                int count = Integer.parseInt(fields[1]);
                return Tuple2.of(word, count);
            }
        });

        //先分组，再划分窗口
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndCount.keyBy(0);
        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> window = keyed.countWindow(5);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = window.sum(1);

        sumed.print();

        env.execute("CountWindow");
    }
}
