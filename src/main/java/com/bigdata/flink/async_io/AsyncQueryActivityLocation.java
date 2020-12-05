package com.bigdata.flink.async_io;

import com.bigdata.flink.utils.FlinkKafkaUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @ author spencer
 * @ date 2020/5/27 16:03
 * 异步查询外部API
 */
public class AsyncQueryActivityLocation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkKafkaUtilsV1.getEnv();
        DataStream<String> lines = FlinkKafkaUtilsV1.createKafkaStream(args, new SimpleStringSchema());

//        SingleOutputStreamOperator<ActivityBean> beans = lines.map(new LocationToActivityBeanFunction());

        //异步调用
        SingleOutputStreamOperator<ActivityBean> result = AsyncDataStream.unorderedWait(lines, new AsyncLocationToActivityBeanFunction(), 0, TimeUnit.MICROSECONDS, 10);
        result.print();

        env.execute("AsyncQueryActivityLocation");
    }
}
