package com.bigdata.flink.async_io;

import com.bigdata.flink.utils.FlinkKafkaUtilsV1;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ author spencer
 * @ date 2020/5/27 16:03
 * 启动flink程序，读取高德地图经纬度所对应的省份信息
 * u001,A1,2020-05-27 14:18:45,1,成都市
 u001,A1,2020-05-27 14:18:45,2,北京市
 u002,A1,2020-05-27 14:18:45,3,上海市
 u003,A2,2020-05-27 14:18:45,1,广州市
 u002,A2,2020-05-27 14:18:45,2,深圳市
 u004,A2,2020-05-27 14:18:45,3,杭州市
 u001,A3,2020-05-27 14:18:45,2,武汉市
 u003,A3,2020-05-27 14:18:45,1,西安市
 u002,A3,2020-05-27 14:18:45,2,天津市
 u005,A3,2020-05-27 14:18:45,3,昆明市

 需求：查询高德地图API，关联地理位置信息

 给定数据：
 u001,A1,2020-05-27 14:18:45,3,115.908923,39.267291
 u001,A1,2020-05-27 14:18:45,3,123.818817,41.312458
 u002,A2,2020-05-27 14:18:45,3,121.26757,37.49794
 u003,A2,2020-05-27 14:18:45,3,104.063402,30.568744
 */
public class QueryActivityLocation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkKafkaUtilsV1.getEnv();
        DataStream<String> lines = FlinkKafkaUtilsV1.createKafkaStream(args, new SimpleStringSchema());

        //同步调用lines.map(new LocationToActivityBeanFunction())
        SingleOutputStreamOperator<ActivityBean> beans = lines.map(new LocationToActivityBeanFunction());

        beans.print();

        env.execute("QueryActivityLocation");
    }
}
