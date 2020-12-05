package com.bigdata.flink.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.*;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/1 15:44
 */
public class FlinkKafkaUtilsV2 {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static <T> DataStream<T> createKafkaStream(
            ParameterTool parameterTool,
            Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        // 设置全局的配置参数
        env.getConfig().setGlobalJobParameters(parameterTool);

        env.enableCheckpointing(parameterTool.getLong("checkpoint-interval"), CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend(parameterTool.getRequired("state.backend")));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameterTool.getRequired("bootstrap.servers"));
        properties.setProperty("group.id",  parameterTool.getRequired("group.id"));
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", parameterTool.get("auto.offset.reset", "earliest"));
        //kafka的消费者不自动提交偏移量,以后自己管理偏移量
        properties.setProperty("enable.auto.commit", parameterTool.get("enable.auto.commit", "false"));

        String topics = parameterTool.getRequired("topics");
        List<String> topicList = Arrays.asList(topics.split(","));

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(
                topicList,
                clazz.newInstance(),
                properties
        );

        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

//        // 指定offset消费的5中模式
//        kafkaConsumer.setStartFromEarliest();
//        kafkaConsumer.setStartFromLatest();
//        kafkaConsumer.setStartFromTimestamp(1000);
//        kafkaConsumer.setStartFromGroupOffsets();

//        Map<KafkaTopicPartition, Long> map = new HashMap<>();
//        KafkaTopicPartition kafkaTopicPartition = new KafkaTopicPartition("wc10", 3);
//        map.put(kafkaTopicPartition, 1000L);
//        kafkaConsumer.setStartFromSpecificOffsets(map);

        return env.addSource(kafkaConsumer);
    }

    public static StreamExecutionEnvironment getEnv(){
        return env;
    }
}
