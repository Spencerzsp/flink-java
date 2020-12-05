package com.bigdata.project.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @ author spencer
 * @ date 2020/6/9 13:51
 */
public class FlinkUtils {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * 读取单个topic流的数据
     * @param parameters
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> DataStream<T> createKafkaStream(
            ParameterTool parameters,
            Class<? extends DeserializationSchema<T>> clazz) throws Exception {
        //设置全局的配置参数
        env.getConfig().setGlobalJobParameters(parameters);

        //开启checkpointing，即开启重启策略
        env.enableCheckpointing(parameters.getLong("checkpoint-interval", 5000L), CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000));

        //设置stateBackend
        env.setStateBackend(new FsStateBackend("file:///D:/IdeaProjects/flink-java/chk002"));

        //取消任务，checkpoint不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", parameters.getRequired("bootstrap.servers"));
        properties.setProperty("group.id", parameters.getRequired("group.id"));
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", parameters.get("auto.offset.reset", "earliest"));
        //kafka的消费者不自动提交偏移量,以后自己管理偏移量
        properties.setProperty("enable.auto.commit", parameters.get("enable.auto.commit", "false"));

        String topics = parameters.getRequired("topics");
        List<String> topicList = Arrays.asList(topics.split(","));

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<>(
                topicList,
                clazz.newInstance(),
                properties
        );

        //flink checkpoint成功后，还要往kafka特殊的topic中写入当前偏移量信息,默认为true
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        return env.addSource(kafkaConsumer);
    }

    /**
     * 读取多个topic流的数据
     * @param parameters
     * @param inputTopics
     * @param groupId
     * @param clazz
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> DataStream<T> createKafkaStream(
            ParameterTool parameters,
            String inputTopics,
            String groupId,
            Class<? extends DeserializationSchema<T>> clazz) throws Exception{
        //设置全局的配置参数
//        env.getConfig().setGlobalJobParameters(parameters);

        //开启checkpointing，即开启重启策略
        env.enableCheckpointing(parameters.getLong("checkpoint-interval", 5000L), CheckpointingMode.EXACTLY_ONCE);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000));

        //设置stateBackend
        env.setStateBackend(new FsStateBackend("file:///D:/IdeaProjects/flink-java/chk002"));

        //取消任务，checkpoint不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",parameters.getRequired("bootstrap.servers"));
        properties.setProperty("group.id", groupId);
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", parameters.get("auto.offset.reset", "earliest"));
        //kafka的消费者不自动提交偏移量,以后自己管理偏移量
        properties.setProperty("enable.auto.commit", parameters.get("enable.auto.commit", "false"));

        List<String> topicList = Arrays.asList(inputTopics.split(","));

        FlinkKafkaConsumer<T> kafkaConsumer = new FlinkKafkaConsumer<T>(
                topicList,
                clazz.newInstance(),
                properties
        );

        //flink checkpoint成功后，还要往kafka特殊的topic中写入当前偏移量信息,默认为true
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        return env.addSource(kafkaConsumer);
    }

    public static StreamExecutionEnvironment getEnv(){
        return env;
    }
}
