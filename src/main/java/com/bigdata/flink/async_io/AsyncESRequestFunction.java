package com.bigdata.flink.async_io;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ author spencer
 * @ date 2020/5/29 14:31
 */
public class AsyncESRequestFunction extends RichAsyncFunction<String, String> {

    List<HttpHost> hosts = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        hosts = new ArrayList<>();
        hosts.add(new HttpHost("wbbigdata00", 9200));

    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

        ElasticsearchSink.Builder<String> esBuilder = new ElasticsearchSink.Builder<>(
                hosts,
                new ElasticsearchSinkFunction<String>() {
                    @Override
                    public void process(String value, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                        Map<String, String> json = new HashMap<>();
                        json.put("data", value);

                        IndexRequest indexRequest = Requests.indexRequest()
                                .index("flink_index")
                                .type("flink_type")
                                .source(json);

                        requestIndexer.add(indexRequest);

                    }
                });


    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
