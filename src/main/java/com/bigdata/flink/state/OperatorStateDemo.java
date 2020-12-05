package com.bigdata.flink.state;

import com.bigdata.flink.sources.MyParaExactlyOnceFileSource;
import com.bigdata.flink.sources.MyParaFileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ author spencer
 * @ date 2020/6/4 15:37
 * 从自定义Source中读取文件中的内容，即实现 tail -f 的功能。
 */
public class OperatorStateDemo {

    public static void main(String[] args) throws Exception {

//        System.setProperty("HADOOP_USER_NAME", "hadoop");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度为2,因为读取的文件夹中就存放了2个文件(0.txt和1.txt)
        env.setParallelism(2);

        //只有开启了CheckPointing,才会有重启策略
        env.enableCheckpointing(5000);

        //设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2000));

        env.setStateBackend(new FsStateBackend("file:///D:\\IdeaProjects\\flink-java\\chk001"));
        //任务在出现异常或者停掉之后，保留checkpoint目录
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //控制异常的发生，观察程序重启后重新消费数据是否只消费一次
//        DataStreamSource<String> lines = env.socketTextStream("wbbigdata00", 8095);
//        lines.map(new MapFunction<String, Object>() {
//            @Override
//            public Object map(String line) throws Exception {
//                if (line.startsWith("hello")) {
//                    System.out.println(1 / 0);
//                }
//                return line;
//            }
//        }).print();

        //MyParaFileSource(path)中的path不能跟上file:///
        //自定义并行且实现ExactlyOnce消费数据
        DataStreamSource<Tuple2<String, String>> source = env.addSource(new MyParaExactlyOnceFileSource("D:/IdeaProjects/flink-java/src/main/resources/data/"));

        source.print();

        env.execute("OperatorStateDemo");
    }
}
