package com.bigdata.project.app.topn;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.pojo.ItemCount;
import com.bigdata.pojo.UserAction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.scala.function.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.runtime.aggregate.AggregateAggFunction;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.util.Properties;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 10:02
 */
public class TopNJob {

    public static final Long MAX_DELAY_EVENT_TIME = 10L;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 设置watermark产生的周期，默认为200ms
        env.getConfig().setAutoWatermarkInterval(1000);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.flink.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.flink.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>(
                "user_behavior",
                new SimpleStringSchema(),
                properties
        ));

//        dataStreamSource.print();

        DataStream<UserAction> userActionDataStream = dataStreamSource.map(value -> JSONObject.parseObject(value, UserAction.class));

//        userActionDataStream.print();

        SingleOutputStreamOperator<UserAction> userActionWaterMarkStream = userActionDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserAction>(Time.seconds(MAX_DELAY_EVENT_TIME)) {
            @Override
            public long extractTimestamp(UserAction element) {
                return element.ts;
            }
        });

        //为了统计5分钟购买的最多的，所以我们需要过滤出购买的行为
        SingleOutputStreamOperator<String> top3Items = userActionWaterMarkStream.filter(new FilterFunction<UserAction>() {
            @Override
            public boolean filter(UserAction userAction) throws Exception {
                return userAction.getBehavior().contains("pv");
            }
        })
                .keyBy("itemId")
                .timeWindow(Time.days(5), Time.hours(1))
                .aggregate(new CountAgg(), new WindowResult())
                .keyBy("windowEnd")
                .process(new TopNHotItems(3));

        top3Items.print();

        env.execute("TopNJob");
    }
}
