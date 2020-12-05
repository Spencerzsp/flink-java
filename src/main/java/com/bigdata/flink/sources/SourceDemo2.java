package com.bigdata.flink.sources;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Arrays;

/**
 * 可以并行的source，即并行度大于1
 * @ author spencer
 * @ date 2020/5/22 15:52
 */
public class SourceDemo2 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> nums = env.fromParallelCollection(new NumberSequenceIterator(1, 10), TypeInformation.of(Long.TYPE));

        DataStreamSource<Long> sequence = env.generateSequence(1, 100);
        int parallelism = nums.getParallelism();
        int parallelism1 = sequence.getParallelism();

        System.out.println("======================================================= " + parallelism);
        System.out.println("======================================================= " + parallelism1);

        nums.print();
        env.execute("SourceDemo");
    }
}
