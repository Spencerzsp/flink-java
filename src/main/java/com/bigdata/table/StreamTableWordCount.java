package com.bigdata.table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @ author spencer
 * @ date 2020/6/12 16:34
 */
public class StreamTableWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                Arrays.stream(line.split(" ")).forEach(word -> collector.collect(word));
            }
        });


        Table table = tableEnv.fromDataStream(words, "word")
                .groupBy("word") // 分组
                .select("word, count(1) as counts");

        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Row.class);

        dataStream.print();

        env.execute("StreamTableWordCount");

    }
}
