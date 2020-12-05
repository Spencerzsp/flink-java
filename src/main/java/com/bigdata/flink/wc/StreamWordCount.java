package com.bigdata.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ author spencer
 * @ date 2020/5/22 14:07
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 主机名和端口号在提交的时候按照参数传进来
        DataStream<String> lines = env.socketTextStream(args[0], Integer.parseInt(args[1]));
//        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String line, Collector<String> collector) throws Exception {
//                String[] words = line.split(" ");
//                for (String word : words) {
//                    collector.collect(word);
//                }
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String word) throws Exception {
//                return Tuple2.of(word, 1);
//            }
//        });

        //直接一步到位
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    collector.collect(tuple2);
                }
            }
        }).slotSharingGroup("doit");

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = wordAndOne.keyBy(0).sum(1);

//        wordAndOne.keyBy(0)
//                //每5s统计一次，即窗口大小为5s
//                .timeWindow(Time.seconds(5))
//                .sum(1)
//                .print();

        sum.print();

        env.execute("StreamTableWordCount");

    }
}
