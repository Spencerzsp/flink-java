package com.bigdata.apps;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.pojo.OrderDetail;
import com.bigdata.pojo.OrderMain;
import com.bigdata.project.utils.FlinkUtils;
import net.sf.json.JSON;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Properties;

/**
 * @ author spencer
 * @ date 2020/6/11 10:32
 *
 * 订单明细表和主表进行Join
 *
 * 使用EventTime划分滚动窗口
 * 使用CoGroup实现左外连接
 *
 * TODO 没有Join上的数据，单独处理
 */
public class OrderJoinApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getEnv();

        //设置使用EventTime作为时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //测试设置并行度为1，生产环境不能设置为1
        env.setParallelism(1);

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        //延迟乱序时间
        long delayTime = parameters.getLong("delay.time", 1000);
        long windowTime = parameters.getLong("window.time", 2000);

        DataStream<String> orderDetailStream = FlinkUtils.createKafkaStream(
                parameters,
                parameters.getRequired("order.detail.topic"),
                parameters.getRequired("order.detail.group.id"),
                SimpleStringSchema.class
        );

        //底层process方法，实现FlatMap+Filter的功能
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
                        if (type.equals("UPDATE") || type.equals("INSERT")) { // 不能使用==
                            orderDetail.setType(type);
                            out.collect(orderDetail);
                        }
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                    //TODO 记录有问题数据
                }
            }
        });

        //提取EventTime，并生成Watermark
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMark = orderDetailBeanStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderDetail>(Time.milliseconds(delayTime)) {
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
//                    e.printStackTrace();
                    //TODO 记录有问题数据
                }
            }
        });

        //提取EventTime，并生成Watermark
        SingleOutputStreamOperator<OrderMain> orderMainWithWarterMark = orderMainBeanStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderMain>(Time.milliseconds(delayTime)) {
            @Override
            public long extractTimestamp(OrderMain element) {
                return element.getUpdate_time().getTime();
            }
        });

        //明细表作为左表
        //左表数据join右表数据，右表数据迟到了，没有join上：再查数据库关联右表数据
        //左表数据迟到了：通过测输出流获取迟到的数据
        DataStream<Tuple2<OrderDetail, OrderMain>> results = orderDetailWithWaterMark.coGroup(orderMainWithWarterMark)
                .where(new KeySelector<OrderDetail, Long>() {
                    @Override
                    public Long getKey(OrderDetail value) throws Exception {
                        return value.getOrder_id(); //取左表的orderId进行等值连接
                    }
                })
                .equalTo(new KeySelector<OrderMain, Long>() {
                    @Override
                    public Long getKey(OrderMain value) throws Exception {
                        return value.getOid();//取右表的orderId进行等值连接
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowTime)))
                .apply(new CoGroupFunction<OrderDetail, OrderMain, Tuple2<OrderDetail, OrderMain>>() {
                    @Override
                    public void coGroup(Iterable<OrderDetail> first, Iterable<OrderMain> second, Collector<Tuple2<OrderDetail, OrderMain>> out) throws Exception {

                        //先选好左表的数据
                        for (OrderDetail orderDetail : first) {
                            boolean isJoined = false;

                            //再选右表的数据
                            for (OrderMain orderMain : second) {
                                out.collect(Tuple2.of(orderDetail, orderMain));
                                isJoined = true;
                            }
                            // join上了的数据，isJoined已经置为了true
                            if (!isJoined) {
                                out.collect(Tuple2.of(orderDetail, null));
                            }
                        }
                    }
                });

        results.print();

        env.execute("OrderJoinApp");

    }
}
