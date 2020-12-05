package com.bigdata.project.app;

import com.alibaba.fastjson.JSONObject;
import com.bigdata.project.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @ author spencer
 * @ date 2020/6/12 11:43
 */
public class FlinkCanalKafkaDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getEnv();

        ParameterTool parameters = ParameterTool.fromPropertiesFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\config3.properties");

        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);

        SingleOutputStreamOperator<String> maped = kafkaStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(line).getJSONArray("data").getJSONObject(0);
                String id = jsonObject.getString("id");
                String clusterEventName = jsonObject.getString("cluster_event_name");
                String date = jsonObject.getString("date");
                String unit = jsonObject.getString("unit");
                String value = jsonObject.getString("value");

                return id + "|" + clusterEventName + "|" + date + "|" + unit + "|" + value;
            }
        });

        maped.print();

        env.execute("FlinkCanalKafkaDemo");

    }
}
