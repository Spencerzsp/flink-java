package com.bigdata.sql.stream;

import com.bigdata.sql.bean.WordCount;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @ author spencer
 * @ date 2020/6/12 15:53
 */
public class StreamSqlWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //word count hadoop spark flink
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                Arrays.stream(line.split(" ")).forEach(word -> collector.collect(word));
            }
        });

        //注册表
        tableEnv.createTemporaryView("t_wordcount", words, "word");

        Table table = tableEnv.sqlQuery("select word, count(1) counts from t_wordcount group by word");

        // (false,WordCount{word='flink', counts=1})
        // (true,WordCount{word='flink', counts=2})
        // false:表示之前的数据，true：表示新增之后更新的数据
        DataStream<Tuple2<Boolean, WordCount>> dataStream = tableEnv.toRetractStream(table, WordCount.class);

//        dataStream.print();

        // 返回新增之后的数据，即为true的数据
        dataStream.filter(new FilterFunction<Tuple2<Boolean, WordCount>>() {
            @Override
            public boolean filter(Tuple2<Boolean, WordCount> tp) throws Exception {

                return tp.f0;
            }
        }).print();

        env.execute("StreamSqlWordCount");
    }
}
