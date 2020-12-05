package com.bigdata.project.app;

import com.bigdata.project.sink.MyRedisSink;
import com.bigdata.project.utils.FlinkUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @ author spencer
 * @ date 2020/6/9 13:15
 */
public class FlinkKafkaToRedis {

    public static void main(String[] args) throws Exception {

//        ParameterTool parameter = ParameterTool.fromArgs(args);
//
//        String groupId = parameter.get("group.id", "g10");
//        String topics = parameter.getRequired("topics");

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> lines = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1);

        sumed.map(new MapFunction<Tuple2<String,Integer>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Tuple2<String, Integer> value) throws Exception {

                return Tuple3.of("WORD_COUNT", value.f0, value.f1.toString());
            }
        }).addSink(new MyRedisSink());

        //查看redis中HASH的数据 hgetall WORD_COUNT

        FlinkUtils.getEnv().execute("FlinkKafkaToRedis");

    }
}
