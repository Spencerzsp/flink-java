package com.bigdata.flink.strategies;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @ author spencer
 * @ date 2020/6/2 13:36
 */
public class StateBackendDemo {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //只有开启了checkpointing，才有重启策略
        //默认的重启策略：固定无限重启
        env.enableCheckpointing(5000);

        //设置重启策略:重启3次，每个2s重启一次
//        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000));
        //设置状态数据存储的后端
        // 也可以在配置文件中配置状态数据存储的后端
//        env.setStateBackend(new FsStateBackend("file:///D:\\IdeaProjects\\flink-java\\src\\main\\resources\\backend"));
        env.setStateBackend(new FsStateBackend("hdfs://wbbigdata00:8020/user/root/backend"));

        //程序异常退出或人为cancel掉，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        DataStreamSource<String> lines = env.socketTextStream(args[0], 5901);
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
