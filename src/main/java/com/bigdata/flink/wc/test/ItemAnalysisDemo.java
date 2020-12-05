package com.bigdata.flink.wc.test;

import com.bigdata.flink.utils.FlinkKafkaUtilsV2;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/3 9:15
 */
public class ItemAnalysisDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkKafkaUtilsV2.getEnv();
        env.setParallelism(1);
        DataStreamSource<String> itemDataStream = env.readTextFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\data\\item.txt");
//        itemDataStream.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String line) throws Exception {
//                String value = line.replaceAll("\\(", "").replaceAll("\\)", "");
//                String[] fields = value.split("\\|");
//                String sessionId = fields[0].split(",")[1];
//                String searchKeywords = fields[1];
//                String startTime = fields[5];
//                String age = fields[6];
//                String sex = fields[8];
//                String city = fields[9];
//                return sessionId +"|" + searchKeywords + "|" + startTime + "|" + age + "|" + sex + "|" + city;
//            }
//        }).print();

//        SingleOutputStreamOperator<Tuple2<String, Integer>> item = itemDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(Tuple2.of("item", 1));
//            }
//        });
//
//        item.keyBy(0).print();

        SingleOutputStreamOperator<Tuple2<String, Integer>> item = itemDataStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of("item", 1);
            }
        });

        // (item, (1,2,3...))
//        item.keyBy(0).sum(1).

//        itemDataStream.map(new MapFunction<String, Integer>() {
//            int cout = 0;
//            @Override
//            public Integer map(String value) throws Exception {
//                cout++;
//                return cout;
//            }
//        }).flatMap(new FlatMapFunction<Integer, Integer>() {
//            int count = 0;
//            List countList = new ArrayList<Integer>();
//            @Override
//            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
//                count++;
//                countList.add(count);
//
//                out.collect(countList.size());
//            }
//        }).print();

        env.execute("ItemAnalysisDemo");
    }
}
