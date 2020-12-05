package com.bigdata.project.app;

import com.bigdata.pojo.ActBean;
import com.bigdata.project.utils.FlinkUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.HashSet;

/**
 * @ author spencer
 * @ date 2020/6/11 13:13
 */
public class ActivityCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getEnv();
        env.setParallelism(1);

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        DataStream<String> lines = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);

        SingleOutputStreamOperator<ActBean> beanDataStream = lines.map(new MapFunction<String, ActBean>() {
            //数据：u001,A1,2020-05-27 14:18:45,1,成都市
            @Override
            public ActBean map(String line) throws Exception {
                String[] fields = line.split(",");
                String uid = fields[0];
                String aid = fields[1];
                String time = fields[2];
                String date = time.split(" ")[0];
                int type = Integer.parseInt(fields[3]);
                String province = fields[4];

                return ActBean.of(uid, aid, date, type, province, 1);
            }
        });

        //按照指定的条件进行分组
//        SingleOutputStreamOperator<ActBean> summed = beanDataStream.keyBy("aid", "type", "time").sum("count");
//
//        summed.print();

        //统计人数(同一个活动不同类型的人数)
        KeyedStream<ActBean, Tuple> keyed = beanDataStream.keyBy("aid", "type");
        // Tuple3<String, String, Integer>:活动、类型、人数
        keyed.map(new RichMapFunction<ActBean, Tuple3<String, Integer, Integer>>() {

            //使用KeyedState
            //当数据越来越大时，不能使用Set,要使用bloom过滤器
            private transient ValueState<HashSet> uidState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ValueStateDescriptor<HashSet> descriptor = new ValueStateDescriptor<HashSet>(
                        "uid_state",
                        HashSet.class
                );

                //使用上下文获取KeyedState
                uidState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public Tuple3<String, Integer, Integer> map(ActBean bean) throws Exception {
                String uid = bean.uid;
                HashSet uids = uidState.value();
                if (uids == null){
                    uids = new HashSet();
                }

                uids.add(uid);

                //更新状态
                uidState.update(uids);

                return Tuple3.of(bean.aid, bean.type, uids.size());
            }
        }).print();

        env.execute("ActivityCount");

    }
}
