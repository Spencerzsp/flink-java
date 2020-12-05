package com.bigdata.flink.transformations;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * @ author spencer
 * @ date 2020/5/25 16:15
 */
public class SplitDemo01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5);
        SplitStream<Integer> splitStream = nums.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {
                List<String> list = new ArrayList<>();
                if (value % 2 == 0) {
                    list.add("even");
                } else {
                    list.add("odd");
                }

                return list;
            }
        });

        splitStream.select("even").print("even");
        //splitStream.select("odd").print("odd");
        //splitStream.select("even", "odd").print("even-odd");

        env.execute("SplitDemo01");
    }
}
