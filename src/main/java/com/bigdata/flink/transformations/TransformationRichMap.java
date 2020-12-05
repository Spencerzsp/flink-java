package com.bigdata.flink.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ author spencer
 * @ date 2020/5/25 11:10
 */
public class TransformationRichMap {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5);
        SingleOutputStreamOperator<Object> result = nums.map(new MapFunction<Integer, Object>() {
            @Override
            public Object map(Integer value) throws Exception {
                return value * 2;
            }
        });

        //RichMapFunction功能更强大
        SingleOutputStreamOperator<Integer> result3 = nums.map(new RichMapFunction<Integer, Integer>() {
            //open，生命周期方法，在构造方法之后，map方法之前，只执行一次
            //初始化一些连接
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public Integer map(Integer value) throws Exception {
                return value * 10;
            }

            //close，资源释放方法，销毁之前执行一次
            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        SingleOutputStreamOperator<Integer> result2 = nums.map(num -> num * 2).returns(Types.INT);

        result2.print();

        env.execute("TransformationRichMap");

    }
}
