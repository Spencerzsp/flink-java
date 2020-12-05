package com.bigdata.project.app;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.project.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * @ description: 读取规则数据，每次重启的时候都要从头开始读取
 * @ author: spencer
 * @ date: 2020/12/4 11:15
 */
public class BroadcastStateDemo {

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(
                parameter,
                "act",
                UUID.randomUUID().toString(), // 随意指定groupId,保证每次读取都是从头开始读取规则数据
                SimpleStringSchema.class
        );
//        kafkaStream.print();

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpDataStream = kafkaStream.process(new ProcessFunction<String, Tuple3<String, String, String>>() {
            @Override
            public void processElement(String line, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(line);
                    String type = jsonObject.getString("type");
                    if (type.equals("INSERT") || type.equals("UPDATE") || type.equals("DELETE")) {

                        JSONArray data = jsonObject.getJSONArray("data");
                        for (int i = 0; i < data.size(); i++) {
                            JSONObject obj = data.getJSONObject(i);
                            String id = obj.getString("id");
                            String name = obj.getString("name");

                            out.collect(Tuple3.of(id, name, type));
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // 定义一个广播的状态描述器
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>(
                "act-state",
                String.class,
                String.class
        );
        BroadcastStream<Tuple3<String, String, String>> broadcastDataStream = tpDataStream.broadcast(descriptor);

        // u001,A1,2020-12-04
        DataStreamSource<String> lines = FlinkUtils.getEnv().socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> tp2DataStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        SingleOutputStreamOperator<Tuple4<String, String, String, String>> connected = tp2DataStream
                .connect(broadcastDataStream)
                .process(new BroadcastProcessFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {

                    // 处理要计算的活动数据
                    @Override
                    public void processElement(Tuple3<String, String, String> input, ReadOnlyContext ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {

                        ReadOnlyBroadcastState<String, String> mapState = ctx.getBroadcastState(descriptor);

                        // u001,A1,2020-12-04
                        String uid = input.f0;
                        String aid = input.f1;
                        String date = input.f2;

                        // 根据活动ID到广播的mapState中进行关联
                        String name = mapState.get(aid);

                        // 关联后的数据输出
                        out.collect(Tuple4.of(uid, aid, name, date));

                    }

                    // 处理广播的规则数据
                    @Override
                    public void processBroadcastElement(Tuple3<String, String, String> value, Context ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {

                        String id = value.f0;
                        String name = value.f1;
                        String type = value.f2;
                        // 新来一条规则数据，就将规则数据添加到内存
                        BroadcastState<String, String> mapState = ctx.getBroadcastState(descriptor);

                        if (type.equals("DELETE")) {
                            mapState.remove(id);
                        } else {
                            mapState.put(id, name);
                        }

                        Iterator<Map.Entry<String, String>> iterator = mapState.iterator();
                        while (iterator.hasNext()) {
                            Map.Entry<String, String> next = iterator.next();
                            System.out.println("Key: " + next.getKey() + ", Value: " + next.getValue());
                        }

                    }
                });
        connected.print();

        FlinkUtils.getEnv().execute("BroadcastStateDemo");
    }
}
