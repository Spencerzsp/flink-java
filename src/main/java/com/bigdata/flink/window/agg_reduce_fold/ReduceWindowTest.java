package com.bigdata.flink.window.agg_reduce_fold;

import com.bigdata.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 11:42
 */
public class ReduceWindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStreamSource<String> lines = env.socketTextStream("localhost", 7777);
//        SingleOutputStreamOperator<UserAction> userActionDataStream = lines.map(new MapFunction<String, UserAction>() {
//            @Override
//            public UserAction map(String value) throws Exception {
//                UserAction userAction = new UserAction();
//                String[] splits = value.split(",");
//                long userId = Long.parseLong(splits[0]);
//                long itemId = Long.parseLong(splits[1]);
//                int categoryId = Integer.parseInt(splits[2]);
//                String behavior = splits[3];
//                long timestamp = Long.parseLong(splits[4]);
//
//                return UserAction.of(userId, itemId, categoryId, behavior, timestamp);
//            }
//        });

        // 加载数据源source
        DataStreamSource<String> lines = env.readTextFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\sensor.txt");

        // 转换数据源成为DataStream<SensorReading>
        SingleOutputStreamOperator<SensorReading> sensorReadingDataStream = lines.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] splits = value.split(",");
                String sensorId = splits[0];
                long timestamp = Long.parseLong(splits[1]);
                double temperaure = Double.parseDouble(splits[2]);

                return SensorReading.of(sensorId, timestamp, temperaure);
            }
        });

        // 分配timestamp和watermark
        SingleOutputStreamOperator<SensorReading> sensorReadingWaterMarkDataStream = sensorReadingDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000;
            }
        });

        // 对数据切分窗口，执行聚合操作
        SingleOutputStreamOperator<Tuple2<Long, SensorReading>> reduceDataStream = sensorReadingWaterMarkDataStream
                .keyBy("sensorId")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .reduce(new MyReduceFunction(), new MyProcessWindowFunction());

        reduceDataStream.print();

        env.execute("ReduceWindowTest");
    }

    /**
     * 自定义预聚合
     */
    private static class MyReduceFunction implements ReduceFunction<SensorReading>{
        @Override
        public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
            return value1.getTemperature() > value2.getTemperature() ? value1 : value2;
        }
    }

    /**
     * 自定义处理函数
     */
    private static class MyProcessWindowFunction extends ProcessWindowFunction<SensorReading, Tuple2<Long, SensorReading>, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<SensorReading> elements, Collector<Tuple2<Long, SensorReading>> out) throws Exception {
            SensorReading sensorReading = elements.iterator().next();
            long windowEnd = context.window().getEnd();

            out.collect(Tuple2.of(windowEnd, sensorReading));
        }
    }
}
