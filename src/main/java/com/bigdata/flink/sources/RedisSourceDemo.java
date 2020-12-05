package com.bigdata.flink.sources;

import com.bigdata.flink.utils.FlinkKafkaUtilsV2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import redis.clients.jedis.Jedis;

import java.util.Map;
import java.util.Set;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/2 15:02
 */
public class RedisSourceDemo extends RichParallelSourceFunction<String>{
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
    public void run(SourceContext<String> ctx) throws Exception {
        Map<String, String> wordCount = jedis.hgetAll("WORD_COUNT");
        Set<String> keySet = wordCount.keySet();
        for (String key : keySet) {
            String value = wordCount.get(key);
            ctx.collect(key + "=" + value);
        }
    }

    @Override
    public void cancel() {
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkKafkaUtilsV2.getEnv();
        env.setParallelism(1);
        DataStreamSource<String> redisSource = env.addSource(new RedisSourceDemo());
        redisSource.print();

        env.execute("RedisSourceDemo");
    }
}
