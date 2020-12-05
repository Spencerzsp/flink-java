package com.bigdata.flink.async_io;

import com.bigdata.flink.utils.FlinkKafkaUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.concurrent.TimeUnit;

/**
 * @ author spencer
 * @ date 2020/5/29 13:32
 u001,A1,2020-05-27 14:18:45,1,115.908923,39.267291
 u001,A1,2020-05-27 14:18:45,3,123.818817,41.312458
 u002,A2,2020-05-27 14:18:45,3,121.26757,37.49794
 u003,A2,2020-05-27 14:18:45,3,104.063402,30.568744
 u001,A1,2020-05-27 14:18:45,1,115.908923,39.267291
 u001,A3,2020-05-27 14:18:45,2,123.818817,41.312458
 u002,A3,2020-05-27 14:18:45,2,121.26757,37.49794
 u003,A2,2020-05-27 14:18:45,1,104.063402,30.568744
 */
public class ActivityCounts2MysqlAndRedis {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkKafkaUtilsV1.getEnv();
        DataStream<String> lines = FlinkKafkaUtilsV1.createKafkaStream(args, new SimpleStringSchema());

        SingleOutputStreamOperator<ActivityBean> beans = AsyncDataStream.unorderedWait(lines, new AsyncLocationToActivityBeanFunction(), 0, TimeUnit.MILLISECONDS, 10);

        //按照多个维度进行统计分析
        //分组和求和的字段必须是ActivityBean中存在的字段
        SingleOutputStreamOperator<ActivityBean> sumed1 = beans.keyBy("aid","eventType").sum("count");
        SingleOutputStreamOperator<ActivityBean> sumed2 = beans.keyBy("aid","eventType", "province").sum("count");

        sumed1.print();
        //同步写入MySQL
        sumed1.addSink(new MysqlSinkFunction());

        //创建redis conf
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPassword("bigdata").build();
        //写入redis
        sumed2.addSink(new RedisSink<ActivityBean>(conf, new RedisActivityBeanMapper()));

        env.execute("ActivityCounts2MysqlAndRedis");
    }

    //静态内部类实现写入redis
    public static class RedisActivityBeanMapper implements RedisMapper<ActivityBean> {
        //调用redis的写入方法
        @Override
        public RedisCommandDescription getCommandDescription() {
            // HSET设置外面的大key为：ACT_COUNT
            // 命令行查看：hgetall ACT_COUNT
            return new RedisCommandDescription(RedisCommand.HSET, "ACT_COUNT");
        }

        //写入redis中的key(即ACT_COUNT中的小key)
        @Override
        public String getKeyFromData(ActivityBean activityBean) {
            return activityBean.aid + "_" + activityBean.eventType + "_" + activityBean.province;
        }

        //写入redis中的value
        @Override
        public String getValueFromData(ActivityBean activityBean) {
            return String.valueOf(activityBean.count);
        }
    }
}
