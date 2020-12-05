package com.bigdata.flink.sources;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Properties;

/**
 * 从kafka中读取source，可以并行的source，可以实现Exactly Once
 * @ author spencer
 * @ date 2020/5/25 10:40
 */
public class KafkaSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000));
        env.setStateBackend(new FsStateBackend(""));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092");
        properties.setProperty("group.id", "wc10");
        //如果有记录偏移量，就从最近开始消费
        properties.setProperty("auto.offset.reset", "latest");
        //kafka的消费者不自动提交偏移量
        properties.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "wc10",
                new SimpleStringSchema(),
                properties
        );
        // Flink checkpoint成功后，还要向kafka特殊的topic中写入偏移量
        // 默认是true
        kafkaSource.setCommitOffsetsOnCheckpoints(false);

        // 人为控制异常
        env.socketTextStream("localhost", 8888)
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        if (value.startsWith("hello")){
                            System.out.println(1 / 0);
                        }
                        return value;
                    }
                }).print();

        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

        kafkaDataStream.print();

        env.execute("KafkaSource");
    }
}
