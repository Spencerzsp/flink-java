package com.bigdata.flink.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/3 13:46
 */
public class FlinkTumblingWindowLeftJoinDemo {

    public static void main(String[] args) throws Exception {
        int windowSize = 10;
        long delay = 5000L; // 对于延迟的数据，如果watermark>=上一个窗口的结束边界，则上一个窗口的数据会被丢弃。
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

        // left join
        leftStream.coGroup(rightStream)
                .where(new LeftSelectKey())
                .equalTo(new RightSelectKey())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
                .apply(new LeftJoin())
                .print();

        env.execute("FlinkTumblingWindowLeftJoinDemo");
    }

    private static class LeftJoin implements CoGroupFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, Tuple5<String, String, String, Long, Long>> {
        @Override
        public void coGroup(Iterable<Tuple3<String, String, Long>> leftElements, Iterable<Tuple3<String, String, Long>> rightElements, Collector<Tuple5<String, String, String, Long, Long>> out) throws Exception {
            for (Tuple3<String, String, Long> leftElement : leftElements) {
                boolean hadElement = false;
                for (Tuple3<String, String, Long> rightElement : rightElements) {
                    out.collect(Tuple5.of(leftElement.f0, leftElement.f1, rightElement.f1, leftElement.f2, rightElement.f2));
                    hadElement = true;
                }

                if (!hadElement){
                    out.collect(Tuple5.of(leftElement.f0, leftElement.f1, null, leftElement.f2, -1L));
                }
            }
        }
    }
}
