package com.bigdata.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/4 15:49
 */
public class TumblingEventTimeWindowsTable {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        // 19999,u1,p1,5
        SingleOutputStreamOperator<Row> rowDataStream = lines.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String line) throws Exception {
                String[] fields = line.split(",");
                long time = Long.parseLong(fields[0]);
                String uid = fields[1];
                String pid = fields[2];
                double money = Double.parseDouble(fields[3]);
                return Row.of(time, uid, pid, money);
            }
        }).returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.DOUBLE));

        SingleOutputStreamOperator<Row> waterMarkRow = rowDataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(Row element) {
                        return (long) element.getField(0);
                    }
                }
        );

        // rowtime.rowtime:固定不变的，对应event time
        tableEnv.createTemporaryView("t_orders", waterMarkRow, "atime, uid, pid, money, rowtime.rowtime");

        Table table = tableEnv.scan("t_orders")
                .window(Tumble.over("10.seconds").on("rowtime").as("win"))
                .groupBy("uid, win")
                .select("uid, win.start, win.end, win.rowtime, money.sum as total");

        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Row.class);
        dataStream.print();

        env.execute("TumblingEventTimeWindowsTable");

    }
}
