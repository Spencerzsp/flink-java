package com.bigdata.flink.join;

import com.bigdata.pojo.SensorReading;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.net.URL;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 15:45
 */
public class StreamJoinTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        URL resource = StreamJoinTest.class.getClassLoader().getResource("sensor.txt");

        DataStreamSource<String> lines = env.readTextFile(resource.getPath());
        SingleOutputStreamOperator<SensorReading> sensorReadingDataStream = lines.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] splits = value.split(",");
                String sensorId = splits[0];
                long timestamp = Long.parseLong(splits[1]);
                double temperature = Double.parseDouble(splits[2]);
                return SensorReading.of(sensorId, timestamp, temperature);
            }
        });

        // 周期性生成watermark
        SingleOutputStreamOperator<SensorReading> sensorReadingWaterMarkDataStream = sensorReadingDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<SensorReading>() {

            long currentMaxTimestamp = 0L;
            private long maxOutOfOrderness = 10000L;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
            }

            @Override
            public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
                return Math.max(currentMaxTimestamp, element.getTimestamp());
            }
        });

        // 过滤出温度高的传感器
        SingleOutputStreamOperator<SensorReading> highTemperatureDataStream = sensorReadingWaterMarkDataStream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                return value.getTemperature() > 35.0;
            }
        });

        // 过滤出温度低于35.0的传感器
        SingleOutputStreamOperator<SensorReading> lowTemperatureDataStream = sensorReadingWaterMarkDataStream.filter(new FilterFunction<SensorReading>() {
            @Override
            public boolean filter(SensorReading value) throws Exception {
                return value.getTemperature() <= 35.0;
            }
        });

        // 合并高低两条流
        DataStream<Object> joinedStream = highTemperatureDataStream
                .join(lowTemperatureDataStream)
                .where((KeySelector<SensorReading, String>) SensorReading::getSensorId)
                .equalTo((KeySelector<SensorReading, String>) SensorReading::getSensorId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(1000)))
                .allowedLateness(Time.seconds(20))
                .apply(new JoinFunction<SensorReading, SensorReading, Object>() {
                    @Override
                    public Object join(SensorReading first, SensorReading second) throws Exception {
                        return "high: " + first + "\n" + "low: " + second;
                    }
                });

        joinedStream.print();
        env.execute("StreamJoinTest");
    }
}
