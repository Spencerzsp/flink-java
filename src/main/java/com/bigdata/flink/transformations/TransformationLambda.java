package com.bigdata.flink.transformations;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @ author spencer
 * @ date 2020/5/25 11:10
 */
public class TransformationLambda {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.fromElements("spark flink hadoop", "flink hive hbase", "flink spark hbase");

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                //第一种方式
//                String[] words = line.split(" ");
//                for (String word : words) {
//                    out.collect(word);
//                }
                //第二种方式
//                Arrays.asList(line.split(" ")).forEach(word -> out.collect(word));
                //第三种方式
                Arrays.stream(line.split(" ")).forEach(out::collect);
            }
        });

        //java中使用lambda表达式必须跟上参数的类型(String line, Collector<String> out)和返回的类型returns(Types.STRING)
        SingleOutputStreamOperator<String> words2 = lines.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(" ")).forEach(out::collect)).returns(Types.STRING);


        lines.flatMap(new RichFlatMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

            }
        });
        words2.print();
        env.execute("TransformationRichMap");

    }
}
