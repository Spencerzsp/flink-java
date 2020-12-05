package com.bigdata.project.join;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.pojo.OrderDetail;
import com.bigdata.pojo.OrderMain;
import com.bigdata.project.utils.FlinkUtils;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/3 14:13
 */
public class OrderJoin {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        // 设置延迟时间
        long delay = 1000L;
        // 设置滚动窗口大小
        int windowSize = 5;
        // 设置使用EventTime作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> orderDetailStream = FlinkUtils.createKafkaStream(
                parameters,
                parameters.getRequired("order.detail.topic"),
                parameters.getRequired("order.detail.group.id"),
                SimpleStringSchema.class
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailBeanStream = orderDetailStream.process(new ProcessFunction<String, OrderDetail>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderDetail> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    //获取操作类型
                    String type = jsonObject.getString("type");
                    //获取data
                    JSONArray data = jsonObject.getJSONArray("data");
                    for (int i = 0; i < data.size(); i++) {
                        OrderDetail orderDetail = data.getObject(i, OrderDetail.class);
                        if (type.equals("UPDATE") || type.equals("INSERT")) {
                            orderDetail.setType(type);
                            out.collect(orderDetail);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMark = orderDetailBeanStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderDetail>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(OrderDetail element) {
                return element.getUpdate_time().getTime();
            }
        });

        DataStream<String> orderMainStream = FlinkUtils.createKafkaStream(
                parameters,
                parameters.getRequired("order.main.topic"),
                parameters.getRequired("order.main.group.id"),
                SimpleStringSchema.class
        );

        SingleOutputStreamOperator<OrderMain> orderMainBeanStream = orderMainStream.process(new ProcessFunction<String, OrderMain>() {
            @Override
            public void processElement(String value, Context ctx, Collector<OrderMain> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    //获取操作类型
                    String type = jsonObject.getString("type");
                    //获取data
                    JSONArray data = jsonObject.getJSONArray("data");
                    for (int i = 0; i < data.size(); i++) {
                        OrderMain OrderMain = data.getObject(i, OrderMain.class);
                        if (type.equals("UPDATE") || type.equals("INSERT")) {
                            OrderMain.setType(type);
                            out.collect(OrderMain);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        SingleOutputStreamOperator<OrderMain> orderMainWithWaterMark = orderMainBeanStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderMain>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(OrderMain element) {
                return element.getUpdate_time().getTime();
            }
        });

        DataStream<Tuple2<OrderDetail, OrderMain>> orderDetailCoGroupOrderMain = orderDetailWithWaterMark.coGroup(orderMainWithWaterMark)
                .where(new KeySelector<OrderDetail, Long>() {
                    @Override
                    public Long getKey(OrderDetail value) throws Exception {
                        return value.getOrder_id();
                    }
                })
                .equalTo(new KeySelector<OrderMain, Long>() {
                    @Override
                    public Long getKey(OrderMain value) throws Exception {
                        return value.getOid();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>>() {
                    @Override
                    public void coGroup(Iterable<OrderDetail> orderDetails, Iterable<OrderMain> orderMains, Collector<Tuple2<OrderDetail, OrderMain>> out) throws Exception {
                        for (OrderDetail orderDetail : orderDetails) {
                            boolean flag = false;
                            for (OrderMain orderMain : orderMains) {
                                out.collect(Tuple2.of(orderDetail, orderMain));
                                flag = true;
                            }
                            if (!flag) {
                                out.collect(Tuple2.of(orderDetail, null));
                            }
                        }
                    }
                });
        orderDetailCoGroupOrderMain.print();

        env.execute("OrderJoinApp");
    }
}
