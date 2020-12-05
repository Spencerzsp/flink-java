package com.bigdata.flink.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

/**
 * @ author spencer
 * @ date 2020/5/22 15:04
 */
public class LambdaStreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 7777);

//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
//            Arrays.stream(line.split(" ")).forEach(word -> {
//                out.collect(Tuple2.of(word, 1));
//            });
//        }).returns(Types.TUPLE(Types.STRING, Types.INT));      // 使用lambda表达式必须添加returns

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> Arrays
                        .stream(line.split(" "))
                        .forEach(word -> out.collect(Tuple2.of(word, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = wordAndOne.keyBy(0).sum(1);
        wordCount.print();

        env.execute("LambdaStreamWordCount");
    }
}
