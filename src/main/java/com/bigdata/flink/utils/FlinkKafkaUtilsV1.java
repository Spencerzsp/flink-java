package com.bigdata.flink.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @ author spencer
 * @ date 2020/5/27 13:21
 */
public class FlinkKafkaUtilsV1 {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static DataStream<String> createKafkaStream(String[] args, SimpleStringSchema simpleStringSchema) {

        String topic = args[0];
        String groupId = args[1];
        String brokerList = args[2];

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerList);
        properties.setProperty("group.id", groupId);
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", "latest");
        //kafka的消费者不自动提交偏移量
        properties.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                topic,
                new SimpleStringSchema(),
                properties
        );

        return env.addSource(kafkaSource);
    }

    public static StreamExecutionEnvironment getEnv(){
        return env;
    }
}
