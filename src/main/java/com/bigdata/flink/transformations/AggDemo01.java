package com.bigdata.flink.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ author spencer
 * @ date 2020/5/25 15:45
 */
public class AggDemo01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(" ");
                String word = fields[0];
                int count = Integer.parseInt(fields[1]);

                return Tuple2.of(word, count);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordCount.keyBy(0);
        //keyed.min(1).print();

        //返回最大值
        //keyed.max(1).print();

        //返回拥有最大值的element
        keyed.maxBy(1).print();

        env.execute("AggDemo01");

    }
}
