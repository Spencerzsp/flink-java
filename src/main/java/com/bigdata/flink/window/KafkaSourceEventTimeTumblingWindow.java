package com.bigdata.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * @ author spencer
 * @ date 2020/5/26 16:29
 * 使用EventTime，划分滚动窗口
 * 如果使用的是并行的Source，例如kafkaSource，创建kafka的topic时有多个分区
 * 每一个窗口中的Source的分区都要满足触发的条件，整个窗口才会触发
 */
public class KafkaSourceEventTimeTumblingWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置系统处理的时间为EventTime，默认为ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "dafa1:9090");
        properties.setProperty("group.id", "gwordcount10");
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", "latest");
        //kafka的消费者不自动提交偏移量
        properties.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "wordcount10",
                new SimpleStringSchema(),
                properties
        );
        FlinkKafkaConsumerBase<String> start = kafkaSource.setStartFromEarliest();

        DataStream<String> lines = env.addSource(kafkaSource)
           //仅仅是从数据中提取一个字段作为eventTime,原来的数据不变
           //Time.seconds(2)延迟2s触发
           .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(String line) {
                        String[] fields = line.split(",");
                        return Long.parseLong(fields[0]);
                    }
                });

        // 自定义watermark
        SingleOutputStreamOperator<String> kafkaSourceAndWaterMark = env.addSource(kafkaSource)
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {

                    Long currentMaxTimestamp = 0L;
                    Long orderMaxTimestamp = 1000L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - orderMaxTimestamp);
                    }

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        currentMaxTimestamp = Math.max(Long.parseLong(element.split(",")[1]), currentMaxTimestamp);
                        return currentMaxTimestamp;
                    }
                });


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                String word = fields[1];
                int count = Integer.parseInt(fields[2]);
                return Tuple2.of(word, count);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndCount.keyBy(0);

        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        window.sum(1).print();

        env.execute("KafkaSourceEventTimeTumblingWindow");
    }
}
