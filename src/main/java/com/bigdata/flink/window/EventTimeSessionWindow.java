package com.bigdata.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * EventTimeSessionWindows：当相邻两条数据的时间间隔大于指定时间时才触发执行
 * @ author spencer
 * @ date 2020/5/26 16:29
 */
public class EventTimeSessionWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置系统处理的时间为EventTime，默认为ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //1590481000,spark,2
        SingleOutputStreamOperator<String> lines = env.socketTextStream("localhost", 8888)
                //仅仅是从数据中提取一个字段作为eventTime,原来的数据不变
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String line) {
                        String[] fields = line.split(",");
                        return Long.parseLong(fields[0]);
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

        //当相邻两条数据的EventTime大于5s，开始触发窗口计算：EventTimeSessionWindows.withGap(Time.seconds(5))
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyed.window(EventTimeSessionWindows.withGap(Time.seconds(5)));

        window.sum(1).print();

        env.execute("EventTimeSessionWindow");
    }
}
