package com.bigdata.flink.transformations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @ author spencer
 * @ date 2020/5/25 11:10
 */
public class TransformationFilter {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5);
        SingleOutputStreamOperator<Integer> odd = nums.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {

                return value % 2 == 1;
            }
        });

        //lambda表达式
        SingleOutputStreamOperator<Integer> odd2 = nums.filter(num -> num % 2 == 1).returns(Types.INT);
        odd2.print();

//        odd.print();
        env.execute("TransformationRichMap");

    }
}
