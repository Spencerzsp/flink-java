package com.bigdata.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/6 15:38
 */
public class BlinkPlannerTableTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 8888);

        Table table = tableEnv.fromDataStream(dataStreamSource);
        Table word = table
                .groupBy("word")
                .select("select word, count(word) as cnt");

        TupleTypeInfo<Tuple> typeInfo = new TupleTypeInfo<>(
                Types.STRING,
                Types.INT
        );

        DataStream<Tuple2<Boolean, Tuple>> tuple2DataStream = tableEnv.toRetractStream(table, typeInfo);

        tableEnv.toRetractStream(word, Row.class).print();

        env.execute("BlinkPlannerTableTest");
    }
}
