package com.bigdata.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @ author spencer
 * @ date 2020/5/25 10:40
 * 目的：用来观察OperatorState和KeyedState
 * kafka消费者消费数据记录偏移量，消费者对应substak使用OperatorState记录偏移量
 * keyBy之后，进行聚合操作(如累加)，这些subtask使用KeyedState记录数据的历史状态，进行历史数据的累加
 */
public class MapWithStateDemoV2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        //实现EXACTLY_ONCE，必须要记录偏移量
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "dafa1:9090");
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

//        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {

                    //自定义一个异常，观察是否能从内存中恢复数据状态
                    if (word.equals("hello")){
                        System.err.println("hello抛出了一个异常！！！");
//                        throw new RuntimeException();
                    }

                    Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                    out.collect(tuple2);
                }
            }
        });

        //为了保证程序出错要继续累加，要记录分组聚合的中间结果
        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);

        //自己实现sum(1)操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化状态或者恢复历史状态

                //定义一个状态描述器
                ValueStateDescriptor descriptor = new ValueStateDescriptor<Integer>(
                        "wc-keyed-state",
                        //TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {})
                        Types.INT
                );
                valueState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

                String word = value.f0;
                Integer count = value.f1;


                Integer total = valueState.value();
                if (total == null){
                    valueState.update(count);
                    return Tuple2.of(word, count);
                } else {
                    total += count;
                    valueState.update(total);

                    return Tuple2.of(word, total);
                }
            }
        });

        sumed.print();

        env.execute("MapWithStateDemo");
    }
}
