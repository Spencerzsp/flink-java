package com.bigdata.flink.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/11/30 13:10
 */
public class WordCountKeyStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            ValueState<Tuple2<String, Integer>> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<Tuple2<String, Integer>>(
                        "word-count",
                        Types.TUPLE(Types.STRING, Types.INT)
                );
                valueState = getRuntimeContext().getState(descriptor);
            }


            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                Tuple2<String, Integer> historyKV = valueState.value();
                String key = value.f0;
                Integer count = value.f1;
                if (historyKV != null){
                    value.f1 += historyKV.f1;
                    valueState.update(value);
                } else {
                    valueState.update(value);
                }
                return value;
            }
        });

//        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.flatMap(new RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
//            ValueState<Tuple2<String, Integer>> valueState;
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//                ValueStateDescriptor<Tuple2<String, Integer>> descriptor = new ValueStateDescriptor<Tuple2<String, Integer>>(
//                        "word-count",
//                        Types.TUPLE(Types.STRING, Types.INT)
//                );
//                valueState = getRuntimeContext().getState(descriptor);
//            }
//
//            @Override
//            public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                // 获取状态中的数据
//                Tuple2<String, Integer> stateData = valueState.value();
//                if (stateData.f0.contains(value.f0)) {
//                    value.f1 += stateData.f1;
//                    valueState.update(value);
//                } else {
//                    valueState.update(value);
//                }
//                out.collect(stateData);
//            }
//
//            @Override
//            public void close() throws Exception {
//                super.close();
//                valueState.clear();
//            }
//        });
        sumed.print();

        env.execute("WordCountKeyStateDemo");
    }
}
