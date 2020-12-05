package com.bigdata.flink.async_io;

import com.bigdata.flink.utils.FlinkKafkaUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ author spencer
 * @ date 2020/5/27 13:22
 * 查询mysql数据封装成ActivityBean
 u001,A1,2020-05-27 14:18:45,1,成都市
 u001,A1,2020-05-27 14:18:45,2,北京市
 u002,A1,2020-05-27 14:18:45,3,上海市
 u003,A2,2020-05-27 14:18:45,1,广州市
 u002,A2,2020-05-27 14:18:45,2,深圳市
 u004,A2,2020-05-27 14:18:45,3,杭州市
 u001,A3,2020-05-27 14:18:45,2,武汉市
 u003,A3,2020-05-27 14:18:45,1,西安市
 u002,A3,2020-05-27 14:18:45,2,天津市
 u005,A3,2020-05-27 14:18:45,3,昆明市
 */
public class QueryActivityName {

    public static void main(String[] args) throws Exception {

        DataStream<String> lines = FlinkKafkaUtilsV1.createKafkaStream(args, new SimpleStringSchema());
        StreamExecutionEnvironment env = FlinkKafkaUtilsV1.getEnv();

        SingleOutputStreamOperator<ActivityBean> beans = lines.map(new DataToActivityBeanFunction());

        beans.print();

        env.execute("QueryActivityName");
    }
}
