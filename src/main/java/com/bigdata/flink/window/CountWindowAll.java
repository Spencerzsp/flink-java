package com.bigdata.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @ author spencer
 * @ date 2020/5/26 15:30
 * 不分组，当window中达到指定数量时触发执行
 */
public class CountWindowAll {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums = lines.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });

        // 不分组，将整体划分为一个组
        //countWindowAll(5),按照窗口中数据的条数触发，攒够一个批次才进行处理
        AllWindowedStream<Integer, GlobalWindow> windowAll = nums.countWindowAll(5);
        SingleOutputStreamOperator<Integer> sumed = windowAll.sum(0);

        sumed.print();

        env.execute("CountWindowAll");
    }
}
