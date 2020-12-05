package com.bigdata.flink.sinks;

import com.bigdata.flink.utils.FlinkKafkaUtilsV2;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.URL;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/2 14:30
 */
// Tuple3<String, String, String>:第一个String：大key，第二个String：小key，第三个String：value
public class RedisSinkDemo extends RichSinkFunction<Tuple3<String, String, String>>{
    private transient Jedis jedis;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 创建redis链接
        String redisHost = parameters.getString("redis.host", "localhost");
        String password = parameters.getString("redis.pwd", "bigdata");
        jedis = new Jedis(redisHost, 6379, 2000);
        jedis.auth(password);
    }

    @Override
    public void invoke(Tuple3<String, String, String> value, Context context) throws Exception {
        if (!jedis.isConnected()){
            jedis.connect();
        }
        jedis.hset(value.f0, value.f1, value.f2);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = FlinkKafkaUtilsV2.getEnv();
//        URL url = RedisSinkDemo.class.getClassLoader().getResource("config.properties");
//        ParameterTool parameter = ParameterTool.fromPropertiesFile(url.getPath());
        // 以参数的形式传递配置文件，方便提交。提交的时候最好不要将配置文件打包到resources下面，
        // 最好放在服务器指定的文件目录下面
        ParameterTool parameter = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> kafkaStream = FlinkKafkaUtilsV2.createKafkaStream(parameter, SimpleStringSchema.class);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = kafkaStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0).sum(1);

        SingleOutputStreamOperator<Tuple3<String, String, String>> maped = sumed.map(new MapFunction<Tuple2<String, Integer>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(Tuple2<String, Integer> value) throws Exception {
                return Tuple3.of("WORD_COUNT", value.f0, value.f1.toString());
            }
        });

        maped.addSink(new RedisSinkDemo());
        maped.print();

        env.execute("RedisSinkDemo");
    }
}
