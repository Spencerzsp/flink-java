package com.bigdata.flink.sinks;

import com.bigdata.pojo.Person;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * flink读取kafka中数据处理后写入es
 * @ author spencer
 * @ date 2020/6/4 17:56
 */
public class FlinkToEs {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092");
        properties.setProperty("group.id", "gwc10");
        //如果没有记录偏移量，第一次从最开始消费
        properties.setProperty("auto.offset.reset", "latest");
        //kafka的消费者不自动提交偏移量
        properties.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(
                "wc10",
                new SimpleStringSchema(),
                properties
        );

        //获取到kafka中的数据，进行transform操作
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = kafkaDataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        })
                .keyBy(0)
                .sum(1);

//        SingleOutputStreamOperator<String> map = sum.map(new MapFunction<Tuple2<String, Integer>, String>() {
//            @Override
//            public String map(Tuple2<String, Integer> wordCount) throws Exception {
//                return wordCount.f0 + " -> " + wordCount.f1;
//            }
//        });


//        ParameterTool parametes = ParameterTool.fromPropertiesFile(args[0]);
//
//        StreamExecutionEnvironment env = FlinkUtils.getEnv();
//        DataStream<String> kafkaSource = FlinkUtils.createKafkaStream(parametes, SimpleStringSchema.class);

        List<HttpHost> esHttphost = new ArrayList<>();
        esHttphost.add(new HttpHost("dafa1", 60010));

        ElasticsearchSink.Builder<Tuple2<String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<Tuple2<String, Integer>>(
                esHttphost,
                new ElasticsearchSinkFunction<Tuple2<String, Integer>>() {

                    @Override
                    public void process(Tuple2<String, Integer> data, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

                        Map<String, String> json = new HashMap<>();
                        String key = data.f0;
                        Integer value = data.f1;
                        json.put("word", key);
                        json.put("count", value.toString());
                        System.out.println(json);
                        IndexRequest indexRequest = Requests.indexRequest()
                                .index("word_count")
                                .type("_doc")
                                .source(json);

                        requestIndexer.add(indexRequest);
                    }
                }
        );

        // 内置自定义配置
        esSinkBuilder.setBulkFlushMaxActions(1);
        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
//                    restClientBuilder.setDefaultHeaders();
//                    restClientBuilder.setMaxRetryTimeoutMillis(Integer.MAX_VALUE);
//                    restClientBuilder.setPathPrefix(...)
//                    restClientBuilder.setHttpClientConfigCallback(...)
                }
        );

//        env.addSource(kafkaSource).addSink(esSinkBuilder.build());

        sum.addSink(esSinkBuilder.build());
        sum.print();

        env.execute("FlinkToEs");
    }
}
