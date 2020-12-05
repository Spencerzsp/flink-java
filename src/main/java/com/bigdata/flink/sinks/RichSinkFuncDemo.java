package com.bigdata.flink.sinks;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.expressions.In;

/**
 * @ author spencer
 * @ date 2020/5/25 13:12
 */
public class RichSinkFuncDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> words = env.socketTextStream(args[0], Integer.parseInt(args[1]));

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyed = wordAndOne.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.sum(1);


        // 定义输出的sink样式
        sumed.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {

//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//            }

            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                //获取subtask下标，底层默认下标从0开始，显示为index+1
                int index = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(index + " -> " + value);
            }
        });

        env.execute("RichSinkFuncDemo");
    }
}
