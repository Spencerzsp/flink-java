package com.bigdata.flink.transformations;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ author spencer
 * @ date 2020/5/25 13:12
 */
public class ReduceDemo01 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        //在java中认为元组也是一个特殊的集合，下标也是从0开始
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduced = keyed.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                String key = value1.f0;
//                Integer count1 = value1.f1;
//                Integer count2 = value2.f1;
//
//                Integer counts = count1 + count2;
//                return Tuple2.of(key, counts);
                value1.f1 += value1.f1 + value2.f1;
                return value1;
            }
        });

        //keyBy之后可以根据相同key的value值进行大小比较
        SingleOutputStreamOperator<Tuple2<String, Integer>> maxed = keyed.max(1);

        reduced.print();

        env.execute("ReduceDemo01");
    }
}
