package com.bigdata.flink.wc.test;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.util.Arrays;

/**
 * @ author spencer
 * @ date 2020/7/17 11:20
 */
public class LambdaStreamWordCountTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = socketTextStream.flatMap(
                (String line, Collector<Tuple2<String, Integer>> out) ->
                        Arrays.stream(line.split(" "))
                                .forEach(word -> Tuple2.of(word, 1))
        ).returns(Types.TUPLE(Types.STRING, Types.INT));

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne.keyBy(0).sum(1);

        sum.print();

        env.execute("LambadaStreamWordCountTest");
    }
}