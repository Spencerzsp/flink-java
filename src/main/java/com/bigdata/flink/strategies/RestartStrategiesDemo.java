package com.bigdata.flink.strategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @ author spencer
 * @ date 2020/6/2 13:36
 */
public class RestartStrategiesDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //只有开启了checkpointing，才有重启策略
        env.enableCheckpointing(5000); //每5s checkpoint一次

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart());

        //设置固定重启的次数,fixedDelayRestart(3, 2000)：尝试重启三次，每次间隔2s，重启三次后，如果程序再报错则挂掉
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));

//        env.getConfig()
//        .setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, //允许5min内重启失败的最大次数
//                Time.of(5, TimeUnit.MINUTES), //用来衡量失败率的时间间隔
//                Time.of(10, TimeUnit.SECONDS) //每隔10s重启一次，5min内可重启30次
//        ));

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {

                if (word.startsWith("hello")) {
                    throw new RuntimeException("hello is coming,程序挂了！！！");
                }
                return Tuple2.of(word, 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOne.keyBy(0).sum(1);

        result.print();

        env.execute("RestartStrategiesDemo");
    }
}
