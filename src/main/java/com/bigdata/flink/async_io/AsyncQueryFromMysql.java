package com.bigdata.flink.async_io;

import com.bigdata.flink.utils.FlinkKafkaUtilsV1;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @ author spencer
 * @ date 2020/5/28 14:12
 */
public class AsyncQueryFromMysql {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkKafkaUtilsV1.getEnv();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> result = AsyncDataStream.unorderedWait(lines, new AsyncMysqlRequestFunction(), 0, TimeUnit.MILLISECONDS, 100);

        result.print();

        env.execute("AsyncQueryFromMysql");
    }
}
