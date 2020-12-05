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
import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashSet;

/**
 * @ author spencer
 * @ date 2020/6/11 13:13
 * 实时分布式全局去重
 * 1.使用HasheSet去重，数据越来越大的时候不适用
 * 2.使用BloomFilter去重，需要定义两个State，一个BloomFilter，一个CountState
 */
public class ActivityCountAdv {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getEnv();

        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);

        DataStream<String> lines = FlinkUtils.createKafkaStream(parameters, SimpleStringSchema.class);

        SingleOutputStreamOperator<ActBean> beanDataStream = lines.map(new MapFunction<String, ActBean>() {
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

        //统计人数(同一个活动不同类型的人数,即：uid不同，aid和type相同)
        KeyedStream<ActBean, Tuple> keyed = beanDataStream.keyBy("aid", "type");
        keyed.map(new RichMapFunction<ActBean, Tuple3<String, Integer, Long>>() {

            //使用KeyedState
            private transient ValueState<BloomFilter> uidState;

            //一个计数的state
            private transient ValueState<Long> countState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //定义一个状态描述器,里面记录的是【BloomFilter】，用于存放去重的数据
                ValueStateDescriptor<BloomFilter> descriptor = new ValueStateDescriptor<BloomFilter>(
                        "uid_state",
                        BloomFilter.class
                );

                //使用上下文获取KeyedState
                uidState = getRuntimeContext().getState(descriptor);

                //定义一个状态描述器【相当于计数器，因为BloomFilter不像HashSet有size方法，不能获取到次数】
                ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<>(
                        "count-state",
                        Long.class
                );

                countState = getRuntimeContext().getState(countDescriptor);

            }

            @Override
            public Tuple3<String, Integer, Long> map(ActBean bean) throws Exception {
                String uid = bean.uid;
                BloomFilter bloomFilter = uidState.value();
                if (bloomFilter == null){
                    //如果为空，初始化一个bloom过滤器
                    bloomFilter = bloomFilter.create(Funnels.unencodedCharsFunnel(), 1000000);
                    countState.update(0L);
                }

                Long counts = countState.value();
                //可以判断一定不包含
                if (!bloomFilter.mightContain(uid)){
                    //将当前用户加入到bloomFilter
                    bloomFilter.put(uid);
                    countState.update(counts += 1);

                }
                //更新状态
                uidState.update(bloomFilter);

                return Tuple3.of(bean.aid, bean.type, counts);
            }
        }).print();

        env.execute("ActivityCount");

    }
}
