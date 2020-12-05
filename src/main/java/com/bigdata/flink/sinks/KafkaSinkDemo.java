package com.bigdata.flink.sinks;

import com.bigdata.flink.utils.FlinkKafkaUtilsV2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.Properties;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/1 17:48
 */
public class KafkaSinkDemo {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = FlinkKafkaUtilsV2.getEnv();
        URL url = KafkaSinkDemo.class.getClassLoader().getResource("config.properties");
        String path = url.getPath();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);
        DataStream<String> kafkaStream = FlinkKafkaUtilsV2.createKafkaStream(parameterTool, SimpleStringSchema.class);

//        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = kafkaStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(0).sum(1);
        sumed.print();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092");

//        properties.setProperty("group.id",  parameterTool.getRequired("group.id"));
        //如果没有记录偏移量，第一次从最开始消费
//        properties.setProperty("auto.offset.reset", parameterTool.get("auto.offset.reset", "earliest"));
        //kafka的消费者不自动提交偏移量,以后自己管理偏移量
//        properties.setProperty("enable.auto.commit", parameterTool.get("enable.auto.commit", "false"));

        // 此种写入kafka的方式异常
//        sumed.addSink(new FlinkKafkaProducer<Tuple2<String, Integer>>(
//                "word_count",
//                new KafkaSerializationSchema<Tuple2<String, Integer>>() {
//
//                    @Override
//                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> element, @Nullable Long timestamp) {
//                        return new ProducerRecord<byte[], byte[]>("word_count", element.toString().getBytes());
//                    }
//                },
//                properties,
//                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
//        )).name("kafka_sink");

        String brokerList = "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092";
        sumed.addSink(new FlinkKafkaProducer<Tuple2<String, Integer>>(
                brokerList,
                "word_count",
                new SerializationSchema<Tuple2<String, Integer>>() {
                    @Override
                    public byte[] serialize(Tuple2<String, Integer> element) {
                        return element.toString().getBytes();
                    }
                }

        )).name("kafka_sink");

        env.execute("KafkaSinkDemo");
    }
}
