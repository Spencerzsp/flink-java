package com.bigdata.flink.transformations;

import com.bigdata.flink.bean.WordCounts;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ author spencer
 * @ date 2020/5/25 13:38
 * 使用封装对象WordCounts
 */
public class KeyByDemo02 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<WordCounts> wordAndOne = words.map(new MapFunction<String, WordCounts>() {
            @Override
            public WordCounts map(String value) throws Exception {
                return WordCounts.of(value, 1L);
            }
        });

        //封装成对象wordCounts后不能使用keyBy(0)
        // 如果想使用keyBy(0),类型只能是tuple
        KeyedStream<WordCounts, Tuple> keyed = wordAndOne.keyBy("word");

        SingleOutputStreamOperator<WordCounts> sumed = keyed.sum("counts");

        sumed.print();

        env.execute("KeyByDemo02");
    }
}
