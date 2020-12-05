package com.bigdata.project.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/6 11:02
 */
public class ParameterUtils {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> dataStreamSource = env.readTextFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\config.properties");
        dataStreamSource.print();

        Properties props = new Properties();
        dataStreamSource.addSink(new FlinkKafkaProducer<String>(
                "topic",
                new SimpleStringSchema(),
                props
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));
        new Tokenizer();

        env.execute("ParameterUtils");

    }

    public static final class Tokenizer extends RichFlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            ParameterTool parameters = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

            String bootstrapServers = parameters.get("bootstrap.servers", "dafa1:9092");
            System.out.println(bootstrapServers + "---------------------------------");
        }
    }

}
