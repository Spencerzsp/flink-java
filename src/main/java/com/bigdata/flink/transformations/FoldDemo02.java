package com.bigdata.flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ author spencer
 * @ date 2020/5/25 15:29
 */
public class FoldDemo02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    out.collect(tuple2);
                }
            }
        });

//        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5);

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = words.keyBy(0);
        SingleOutputStreamOperator<String> result = keyed.fold("start", new FoldFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String fold(String accumulator, Tuple2<String, Integer> value) throws Exception {
                Integer count = value.f1;
                String word = value.f0;
                return accumulator + "-" + word + "-" + count;
            }
        });

        result.print();

        env.execute("FoldDemo02");
    }
}
