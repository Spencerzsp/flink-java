package com.bigdata.project.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 从kafka中读取source，可以并行的source，可以实现Exactly Once
 * @ author spencer
 * @ date 2020/5/25 10:40
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启checkpointing，即开启重启策略
        env.enableCheckpointing(5000);

        //设置stateBackend
        env.setStateBackend(new FsStateBackend("file:///D:/IdeaProjects/flink-java/chk002"));

        //取消任务，checkpoint不删除
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //设置checkpoint模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "dafa1:9090");
        properties.setProperty("group.id", "mytest");
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", "earliest");
        //kafka的消费者不自动提交偏移量,以后自己管理偏移量
        properties.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "mytest10",
                new SimpleStringSchema(),
                properties
        );

        //flink checkpoint成功后，还要往kafka特殊的topic中写入当前偏移量信息
        kafkaSource.setCommitOffsetsOnCheckpoints(true);

        DataStream<String> lines = env.addSource(kafkaSource);

        lines.print();

        env.execute("KafkaSource");
    }
}
