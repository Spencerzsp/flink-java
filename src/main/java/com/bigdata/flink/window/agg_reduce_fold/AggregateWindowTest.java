package com.bigdata.flink.window.agg_reduce_fold;

import com.bigdata.pojo.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
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
 * @ date: 2020/8/5 13:52
 */
public class AggregateWindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
                return element.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Double>> aggregateStream = sensorReadingWaterMarkDataStream
                .keyBy("sensorId")
                .timeWindow(Time.hours(1), Time.seconds(5))
                .aggregate(new AverageAggregate(), new MyProcessWindowFunction());

        aggregateStream.print();
        env.execute("AggregateWindowTest");
    }

    /**
     * 自定义聚合函数：注意数据类型是否匹配
     */
    private static class AverageAggregate implements AggregateFunction<SensorReading, Tuple2<Long, Long>, Double> {

        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(SensorReading value, Tuple2<Long, Long> accumulator) {
            long temp = Math.round(value.getTemperature());
            return Tuple2.of(accumulator.f0 + temp, accumulator.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return Double.valueOf(accumulator.f0 / accumulator.f0);
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

//    private static class MyProcessWindowFunction extends ProcessWindowFunction<Double, Tuple2<String, Double>, String, TimeWindow>{
//        @Override
//        public void process(String key, Context context, Iterable<Double> elements, Collector<Tuple2<String, Double>> out) throws Exception {
//            Double average = elements.iterator().next();
//            out.collect(Tuple2.of(key, average));
//        }
//    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Double, Tuple2<String, Double>, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Double> elements, Collector<Tuple2<String, Double>> out) throws Exception {
//            String key = ((Tuple1<String>) tuple).f0;
            String key = tuple.toString();
            Double average = elements.iterator().next();
//            out.collect(Tuple2.of(key, average));
            out.collect(new Tuple2<>(key, average));
        }
    }
}
