package com.bigdata.flink.sinks;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @ author spencer
 * @ date 2020/5/26 9:43
 */
public class FileSinkDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    out.collect(tuple2);
                }
            }
        })
                //将各个算子单独划分处理
                .disableChaining()
                //表示开启一个新链,发生Redistributing
                .startNewChain();

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne
                .keyBy(0)
                .sum(1);
        sumed.writeAsCsv("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\output", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("FileSinkDemo");
    }
}
