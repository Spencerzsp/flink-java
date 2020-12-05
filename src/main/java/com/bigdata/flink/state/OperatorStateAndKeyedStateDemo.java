package com.bigdata.flink.state;

import com.bigdata.flink.conf.ConfigurationManager;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @ author spencer
 * @ date 2020/5/25 10:40
 * 目的：用来观察OperatorState和KeyedState
 * kafka消费者消费数据记录偏移量，消费者对应substak使用OperatorState记录偏移量
 * keyBy之后，进行聚合操作(如累加)，这些subtask使用KeyedState进行历史数据的累加
 */
public class OperatorStateAndKeyedStateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //实现EXACTLY_ONCE，必须要记录偏移量
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092");
        properties.setProperty("group.id", "gwc10");
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", "earliest");
        //kafka的消费者不自动提交偏移量，而是交给flink的checkpointing管理
        properties.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "wc10",
                new SimpleStringSchema(),
                properties
        );

        DataStream<String> lines = env.addSource(kafkaSource);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    out.collect(tuple2);
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

//        keyed.process(new KeyedProcessFunction<String, Tuple2<String,Integer>, String>() {
//            @Override
//            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<String> out) throws Exception {
//                ctx.timerService().registerEventTimeTimer(Long.parseLong(value.f0));
//                out.collect(value.f0);
//            }
//        })

        //为了保证程序出错要继续累加，要记录分组聚合的中间结果
//        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.sum(1);

        sumed.print();

        env.execute("OperatorStateAndKeyedStateDemo");
    }
}
