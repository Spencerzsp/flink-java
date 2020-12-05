package com.bigdata.flink.utils;

import com.bigdata.flink.conf.ConfigurationManager;
import com.bigdata.flink.constant.Constant;
import net.sf.json.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.net.URL;

/**
 * @ author spencer
 * @ date 2020/5/26 10:52
 */
public class ParamUtils {

    public static String getParam(JSONObject jsonObject, String field){
        return jsonObject.getString(field);
    }

    public static void main(String[] args) throws Exception {

//        String jsonStr = ConfigurationManager.config.getString(Constant.TASK_PARAMS_JSON);
//        JSONObject taskParam = JSONObject.fromObject(jsonStr);
//
//        String startDate = getParam(taskParam, "startDate");
//        System.out.println(startDate);
        System.setProperty("HADOOP_USER_NAME", "root");

        URL url = ParamUtils.class.getClassLoader().getResource("config.properties");
        String path = url.getPath();
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(path);
        DataStream<String> kafkaStream = FlinkKafkaUtilsV2.createKafkaStream(parameterTool, SimpleStringSchema.class);
        kafkaStream.print();

        FlinkKafkaUtilsV2.getEnv().execute("ParamUtils");
    }
}
