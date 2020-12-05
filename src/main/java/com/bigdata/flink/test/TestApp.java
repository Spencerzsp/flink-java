package com.bigdata.flink.test;

import com.bigdata.pojo.SensorReading;
import com.iteblog.ElasticSearchOutputFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ author spencer
 * @ date 2020/6/11 9:51
 */
public class TestApp {

    public static void main(String[] args) throws Exception {

        Logger logger = LoggerFactory.getLogger(TestApp.class);

        logger.info("this is logger!");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        DataSource<SensorReading> dataSource1 = env.readCsvFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\sensor.txt")
//                .pojoType(SensorReading.class, "sensorId", "timestamp", "temperature");
//
//        DataSource<String> stringDataSource = env.readTextFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\sensor.txt");
//        FlatMapOperator<String, SensorReading> datasource2 = stringDataSource.flatMap(new FlatMapFunction<String, SensorReading>() {
//            @Override
//            public void flatMap(String value, Collector<SensorReading> out) throws Exception {
//                String[] fields = value.split(",");
//                String sensorId = fields[0];
//                long timestamp = Long.parseLong(fields[1]);
//                double temperature = Double.parseDouble(fields[2]);
//
//                out.collect(SensorReading.of(sensorId, timestamp, temperature));
//            }
//        });

//        datasource2.writeAsFormattedText(
//                "D:\\IdeaProjects\\flink-java\\src\\main\\resources\\test\\b",
//                new TextOutputFormat.TextFormatter<SensorReading>() {
//                    @Override
//                    public String format(SensorReading sensorReading) {
//                        String sensorId = sensorReading.getSensorId();
//
//                        return sensorId + "--->" + sensorReading;
//                    }
//                });

//        MapOperator<String, Row> mapDataSource = stringDataSource.map(new MapFunction<String, Row>() {
//            @Override
//            public Row map(String value) throws Exception {
//                String[] fields = value.split(",");
//                String sensorId = fields[0];
//
//                // 将Long类型转换为Timestamp的类型，并指定格式
//                long time = Long.parseLong(fields[1]);
//                Date date = new Date(time);
//                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                Timestamp timestamp = Timestamp.valueOf(sdf.format(date));
//
//                double temperature = Double.parseDouble(fields[2]);
//                Row row = new Row(3);
//                row.setField(0, sensorId);
//                row.setField(1, timestamp);
//                row.setField(2, temperature);
//                return row;
//            }
//        });

        // 准备使用JDBCOutputFormat写入数据到mysql,类似spark写入mysql
//        mapDataSource.output(
//                JDBCOutputFormat.buildJDBCOutputFormat()
//                .setDrivername("com.mysql.jdbc.Driver")
//                .setDBUrl("jdbc:mysql://wbbigdata01:3306/test")
//                .setUsername("root")
//                .setPassword("bigdata")
//                .setQuery("insert into sensor values (?, ?, ?)")
//                .setSqlTypes(new int[]{Types.VARCHAR, Types.TIMESTAMP, Types.DOUBLE})
//                .finish()
//        );

        // 读取mysql中的数据
        /*
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键ID',
  `article_id` varchar(100) NOT NULL COMMENT '文章编号',
  `collect_website_name` varchar(255) DEFAULT NULL COMMENT '采集网站名称',
  `original_website_name` varchar(255) DEFAULT NULL COMMENT '原网站名称',
  `url` longtext CHARACTER SET utf8mb4 COMMENT '新闻链接',
  `title` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '新闻标题',
  `abstract` longtext CHARACTER SET utf8mb4 COMMENT '新闻摘要',
  `content` longtext CHARACTER SET utf8mb4 COMMENT '新闻内容',
  `pub_time` varchar(50) DEFAULT NULL COMMENT '新闻发布时间',
  `weibo_issue_time` varchar(50) DEFAULT NULL COMMENT '原微博发布时间',
  `insert_time` varchar(50) DEFAULT NULL COMMENT '数据插入时间',
  `author` varchar(50) DEFAULT NULL COMMENT '文章作者',
  `topic` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '微博话题',
  `channel` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '发布渠道',
  `pradeNum` bigint(20) DEFAULT '0' COMMENT '浏览、点赞量',
  `commentNum` bigint(20) DEFAULT '0' COMMENT '评论回复量',
  `transformNum` bigint(20) DEFAULT '0' COMMENT '转发量',
  `ip` varchar(50) DEFAULT NULL COMMENT '采集IP',
  `location` varchar(50) DEFAULT NULL COMMENT '定位',
  `data_source` varchar(50) DEFAULT NULL COMMENT '数据来源。包括：新闻、电子报刊、移动客户端、政务、外媒、微博、微信、论坛、博客9类',
  `is_auth` varchar(100) DEFAULT NULL COMMENT '是否认证（is_auth仅微博）',
  `fansnum` varchar(20) DEFAULT NULL COMMENT '粉丝量（fansnum仅微博）',
  `weibo_media` varchar(255) DEFAULT NULL COMMENT '认证标签（media仅微博）',
  `key_word` varchar(255) DEFAULT NULL COMMENT '采集关键词',
  `is_read` tinyint(4) DEFAULT '0' COMMENT '是否已读，0未读，1已读',
  `Category` varchar(50) DEFAULT NULL COMMENT '类型',
         */
        DataSource<Row> input = env.createInput(
                JDBCInputFormat.buildJDBCInputFormat()
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://wbbigdata01:3306/sinan_opinion1")
                        .setUsername("root")
                        .setPassword("bigdata")
                        .setQuery("select * from opin_bd_cl_init_data")
                        .setRowTypeInfo(new RowTypeInfo(
                                BasicTypeInfo.INT_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                // -------
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                //---
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                //---
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.INT_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO
                        ))
                        .finish()
        );

        input.print();


//        // 创建esSinkBuilder
//        List<HttpHost> esHttphost = new ArrayList<>();
//        esHttphost.add(new HttpHost("localhost", 9200));
//
//        ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<Row>(
//                esHttphost,
//                new ElasticsearchSinkFunction<Row>() {
//
//                    @Override
//                    public void process(Row data, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
//
//                        Map<String, Object> json = new HashMap<>();
//                        int id = (int)data.getField(0);
//                        String article_id = (String)data.getField(1) ;
//                        String collect_website_name = (String)data.getField(2);
//                        json.put("id", id);
//                        json.put("article_id",article_id);
//                        json.put("collect_website_name",collect_website_name);
//                        System.out.println(json);
//                        IndexRequest indexRequest = Requests.indexRequest()
//                                .index("init_data")
//                                .type("_doc")
//                                .source(json);
//
//                        requestIndexer.add(indexRequest);
//                    }
//                }
//        );
//
//        // 内置自定义配置
//        esSinkBuilder.setBulkFlushMaxActions(1);
//        esSinkBuilder.setRestClientFactory(
//                restClientBuilder -> {
////                    restClientBuilder.setDefaultHeaders();
////                    restClientBuilder.setMaxRetryTimeoutMillis(Integer.MAX_VALUE);
////                    restClientBuilder.setPathPrefix(...)
////                    restClientBuilder.setHttpClientConfigCallback(...)
//                }
//        );


        //#######################################################
//        Map<String, String> config = new HashMap<>();
//        config.put("bulk.flush.max.actions", "1000");
//        config.put("cluster.name", "elasticsearch");
//
//        String hosts = "wbbigdata00,wbbigdata01,wbbigdata02";
//        List<InetSocketAddress> list = new ArrayList();
//        for (String host : hosts.split(",")) {
//            list.add(new InetSocketAddress(InetAddress.getByName(host), 9300));
//        }
//        input.output(new ElasticSearchOutputFormat<>(
//                config,
//                list,
//                new com.iteblog.ElasticsearchSinkFunction<Row>() {
//                    @Override
//                    public void process(Row row, RuntimeContext runtimeContext, com.iteblog.RequestIndexer requestIndexer) {
//                        Map<String, Object> json = new HashMap<>();
//                        String id = (String) row.getField(0);
//                        String article_id = (String)row.getField(1) ;
//                        String collect_website_name = (String)row.getField(2);
//                        json.put("id", id);
//                        json.put("article_id",article_id);
//                        json.put("collect_website_name",collect_website_name);
//
//                        IndexRequest indexRequest = Requests.indexRequest().index("init_data2").type("_doc").source(json);
//                        requestIndexer.add(indexRequest);
//                    }
//                }));

    }
}
