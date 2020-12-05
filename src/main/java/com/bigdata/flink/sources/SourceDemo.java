package com.bigdata.flink.sources;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @ author spencer
 * @ date 2020/5/22 15:52
 *
 * 单并行的source（并行度为1）
 */
public class SourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> elements = env.fromElements(1, 2, 3, 4, 5);

        int parallelism = elements.getParallelism();
        System.out.println("=======================" + parallelism);
//        elements.print();

        DataStream<Integer> collection = env.fromCollection(Arrays.asList(1, 2, 3, 4));
//        SingleOutputStreamOperator<Integer> filter = collection.filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer num) throws Exception {
//                return num % 2 == 0;
//            }
//        });

        SingleOutputStreamOperator<Integer> filter = collection.filter((num) -> {
            if (num % 2 == 0) {
                return true;
            } else {
                return false;
            }
        });

//        filter.setParallelism(3);
        int parallelism2 = filter.getParallelism();
        System.out.println("++++++++++++++++++++" + parallelism2);

        filter.print();

        env.execute("SourceDemo");
    }
}
