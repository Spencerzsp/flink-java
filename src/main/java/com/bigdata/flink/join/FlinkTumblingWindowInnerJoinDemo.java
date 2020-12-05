package com.bigdata.flink.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @ description: inner join:两条流中同时在一个窗口中，且key相同的才输出
 * @ author: spencer
 * @ date: 2020/12/3 11:49
 */
public class FlinkTumblingWindowInnerJoinDemo {

    public static void main(String[] args) throws Exception {

        int windowSize = 10;
        long delay = 5002L; // 对于延迟的数据，如果watermark>=上一个窗口的结束边界，则上一个窗口的数据会被丢弃。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> leftSource = env.addSource(new StreamDataSourceA()).name("SourceA");
        SingleOutputStreamOperator<Tuple3<String, String, Long>> rightSource = env.addSource(new StreamDataSourceB()).name("SourceB");

        SingleOutputStreamOperator<Tuple3<String, String, Long>> leftStream = leftSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });
        SingleOutputStreamOperator<Tuple3<String, String, Long>> rightStream = rightSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<String, String, Long>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element) {
                return element.f2;
            }
        });

        // 将两条流进行join
        DataStream<Tuple5<String, String, String, Long, Long>> joinedStream = leftStream.join(rightStream)
                .where(new LeftSelectKey())
                .equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new JoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>>() {
                    @Override
                    public Tuple5<String, String, String, Long, Long> join(Tuple3<String, String, Long> first, Tuple3<String, String, Long> second) throws Exception {
                        return Tuple5.of(first.f0, first.f1, second.f1, first.f2, second.f2);
                    }
                });

        joinedStream.print();

        env.execute("FlinkTumblingWindowInnerJoinDemo");
    }
}
