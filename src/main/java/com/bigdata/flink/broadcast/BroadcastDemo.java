package com.bigdata.flink.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.expressions.In;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/9/27 14:13
 */
public class BroadcastDemo {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取要进行广播的变量
        List<Tuple2<Integer, String>> rawBroadcastData = new ArrayList<>();
        rawBroadcastData.add(Tuple2.of(1, "jack"));
        rawBroadcastData.add(Tuple2.of(2, "tom"));
        rawBroadcastData.add(Tuple2.of(3, "marry"));
        DataSource<Tuple2<Integer, String>> userInfoBroadcastData = env.fromCollection(rawBroadcastData);

        List<Tuple2<Integer, Double>> rawUserAmount = new ArrayList<>();
        rawUserAmount.add(Tuple2.of(1, 1000.00));
        rawUserAmount.add(Tuple2.of(2, 800.00));
        rawUserAmount.add(Tuple2.of(3, 500.00));

        // 模拟数据源(id, amount)
        DataSource<Tuple2<Integer, Double>> userAmount = env.fromCollection(rawUserAmount);

        // 将需要广播的变量转换为集合类型,方便根据id获取userName
        MapOperator<Tuple2<Integer, String>, HashMap<Integer, String>> userInfoBroadcast = userInfoBroadcastData.map(new MapFunction<Tuple2<Integer, String>, HashMap<Integer, String>>() {
            @Override
            public HashMap<Integer, String> map(Tuple2<Integer, String> value) throws Exception {
                HashMap<Integer, String> map = new HashMap<>();
                Integer f0 = value.f0;
                String f1 = value.f1;
                map.put(f0, f1);
                return map;
            }
        });

        // 处理数据，并获取广播变量中的值
        MapOperator<Tuple2<Integer, Double>, String> result = userAmount.map(new RichMapFunction<Tuple2<Integer, Double>, String>() {
            // 创建存放广播变量的集合
            List<HashMap<String, String>> broadcastList = new ArrayList<>();
            // 创建存放广播变量值的map
            Map<String, String> allMap = new HashMap<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                broadcastList = getRuntimeContext().getBroadcastVariable("userInfoBroadcast");
                for (HashMap<String, String> broadcast : broadcastList) {
                    allMap.putAll(broadcast);
                }
            }

            @Override
            public String map(Tuple2<Integer, Double> value) throws Exception {
                String userName = allMap.get(value.f0);
                return "id: " + value.f0 + "用户名: " + userName + "总金额: " + value.f1;
            }
        }).withBroadcastSet(userInfoBroadcast, "userInfoBroadcast");

        result.print();

    }
}
