package com.bigdata.flink.transformations;

import com.bigdata.flink.bean.OrderBean;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ author spencer
 * @ date 2020/5/25 13:12
 */
public class KeyByDemo03 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        // 辽宁,沈阳,1000
        // 山东,青岛,2000
        // 山东,青岛,2000

//        SingleOutputStreamOperator<Tuple3<String, String, Double>> provinceCityAndMoney = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
//            @Override
//            public Tuple3<String, String, Double> map(String value) throws Exception {
//                String[] fields = value.split(",");
//                String province = fields[0];
//                String city = fields[1];
//                double money = Double.parseDouble(fields[2]);
//                Tuple3<String, String, Double> tuple3 = Tuple3.of(province, city, money);
//
//                return tuple3;
//            }
//        });
//
//        SingleOutputStreamOperator<Tuple3<String, String, Double>> sumed = provinceCityAndMoney.keyBy(0, 1).sum(2);

        //使用自定义OrderBean进行封装
        SingleOutputStreamOperator<OrderBean> provinceCityAndMoney = lines.map(new MapFunction<String, OrderBean>() {
            @Override
            public OrderBean map(String value) throws Exception {
                String[] fields = value.split(",");
                String province = fields[0];
                String city = fields[1];
                double money = Double.parseDouble(fields[2]);
                return OrderBean.of(province, city, money);
            }
        });

        KeyedStream<OrderBean, Tuple> keyed = provinceCityAndMoney.keyBy("province", "city");
        SingleOutputStreamOperator<OrderBean> sumed = keyed.sum("money");

        sumed.print();

        env.execute("KeyByDemo03");
    }
}
