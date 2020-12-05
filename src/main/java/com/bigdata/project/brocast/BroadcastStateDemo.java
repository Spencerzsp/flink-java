package com.bigdata.project.brocast;

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

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * @ author spencer
 * @ date 2020/6/11 14:40
 *
 * 使用BroadcastState实时关联处理数据
 * 将MySQL中数据的变换情况进行实时更新处理(使用Canal同步消息到Kafka，Flink获取Socket数据流关联处理Kafka中数据)
 */
public class BroadcastStateDemo {

    public static void main(String[] args) throws Exception {

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);
        DataStream<String> kafkaStream = FlinkUtils.createKafkaStream(
                parameters,
                "dic",
                UUID.randomUUID().toString(), //更换组id，每次重新读取数据
                SimpleStringSchema.class
        );

        SingleOutputStreamOperator<Tuple3<String, String, String>> tpDataStream = kafkaStream.process(new ProcessFunction<String, Tuple3<String, String, String>>() {
            @Override
            public void processElement(String line, Context ctx, Collector<Tuple3<String, String, String>> out) throws Exception {
                JSONObject jsonObject = JSONObject.parseObject(line);
                JSONArray jsonArray = jsonObject.getJSONArray("data");

                String type = jsonObject.getString("type");
                if ("INSERT".equals(type) || "UPDATE".equals(type) || "DELETE".equals(type)) {

                    for (int i = 0; i < jsonArray.size(); i++) {
                        JSONObject obj = jsonArray.getJSONObject(i);
                        String id = obj.getString("id");
                        String name = obj.getString("name");

                        out.collect(Tuple3.of(id, name, type));
                    }
                }
            }
        });

        //定义一个广播的状态描述器
        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>(
                "dic-state",
                String.class,
                String.class
        );

        //将DataStream广播出去
        BroadcastStream<Tuple3<String, String, String>> broadcastState = tpDataStream.broadcast(stateDescriptor);

        //输入数据进行关联
        DataStreamSource<String> lines = FlinkUtils.getEnv().socketTextStream("localhost", 8888);
        //uid01,A1,2020-05-01
        SingleOutputStreamOperator<Tuple3<String, String, String>> tp2DataStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        //要关联已经广播出去的数据
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> connected = tp2DataStream.connect(broadcastState)
                .process(new BroadcastProcessFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {
                    //处理要计算的活动数据
                    @Override
                    public void processElement(Tuple3<String, String, String> input, ReadOnlyContext ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {

                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);

                        String uid = input.f0;
                        String aid = input.f1;
                        String date = input.f2;

                        //根据活动id到广播的state中获取数据
                        String name = state.get(aid);

                        out.collect(Tuple4.of(uid, aid, name, date));

                    }

                    //处理规则数据，即广播出去的数据
                    @Override
                    public void processBroadcastElement(Tuple3<String, String, String> tp, Context ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                        String id = tp.f0;
                        String name = tp.f1;
                        String type = tp.f2;

                        //新来一条规则数据，就将规则数据添加到内存
                        BroadcastState<String, String> state = ctx.getBroadcastState(stateDescriptor);
                        if ("DELETE".equals(type)) {
                            state.remove(id);
                        } else {
                            state.put(id, name);
                        }

                        Iterator<Map.Entry<String, String>> iterator = state.iterator();
                        while (iterator.hasNext()){
                            Map.Entry<String, String> next = iterator.next();
                            System.out.println("key: " + next.getKey() + " value: " + next.getValue());
                        }

                    }
                });

        connected.print();

        FlinkUtils.getEnv().execute("BroadcastStateDemo");
    }
}
