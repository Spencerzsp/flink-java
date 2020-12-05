package com.bigdata.flink.conf;


import com.bigdata.flink.utils.ParamUtils;
import net.sf.json.JSONObject;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.io.IOException;
import java.net.URL;

/**
 * @ author spencer
 * @ date 2020/5/26 11:03
 * java版本ConfigurationManager读取配置文件
 */
public class ConfigurationManager {

    private static Configurations configs = new Configurations();
    public static Configuration config = null;
    static {
        try {
            config = configs.properties(new File("commerce.properties"));
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {

//        String jsonStr = ConfigurationManager.config.getString("task.params.json");
//        System.out.println(jsonStr);
//
//        JSONObject taskParam = JSONObject.fromObject(jsonStr);
//        String endDate = ParamUtils.getParam(taskParam, "endDate");
//        System.out.println(endDate);
//        String targetPageFlow = ParamUtils.getParam(taskParam, "targetPageFlow");
//        System.out.println(targetPageFlow);
//        new ConfigurationManager().getConfig();
//        String jsonStr = config.getString("task.params.json");
//        System.out.println(jsonStr);
//        JSONObject taskParam = JSONObject.fromObject(jsonStr);
//        String endDate = ParamUtils.getParam(taskParam, "endDate");
//
//        System.out.println(endDate);

        ParameterTool parameterTool = ParameterTool.fromPropertiesFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\commerce.properties");
        String s = parameterTool.get("task.params.json");

        JSONObject jsonObject = JSONObject.fromObject(s);
        String startDate = jsonObject.getString("startDate");
        String targetPageFlow = jsonObject.getString("targetPageFlow");
        System.out.println(startDate);
        System.out.println(targetPageFlow);

        System.out.println(s);
    }
}
