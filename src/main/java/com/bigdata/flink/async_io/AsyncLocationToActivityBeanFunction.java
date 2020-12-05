package com.bigdata.flink.async_io;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * @ author spencer
 * @ date 2020/5/28 13:20
 */
public class AsyncLocationToActivityBeanFunction extends RichAsyncFunction<String, ActivityBean> {

    private transient CloseableHttpAsyncClient httpAsyncClient = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //初始化异步的HttpClient
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                .build();

        httpAsyncClient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)
                .setDefaultRequestConfig(requestConfig)
                .build();

        httpAsyncClient.start();
    }

    /*  数据格式
        u001,A1,2020-05-27 14:18:45,3,115.908923,39.267291
        u001,A1,2020-05-27 14:18:45,3,123.818817,41.312458
        u002,A2,2020-05-27 14:18:45,3,121.26757,37.49794
        u003,A2,2020-05-27 14:18:45,3,104.063402,30.568744
     */
    @Override
    public void asyncInvoke(String line, ResultFuture<ActivityBean> resultFuture) throws Exception {
        String[] fields = line.split(",");
        String uid = fields[0];
        String aid = fields[1];
        String time = fields[2];
        int eventType = Integer.parseInt(fields[3]);
        double longitude = Double.parseDouble(fields[4]);
        double latitude = Double.parseDouble(fields[5]);

        String url = "https://restapi.amap.com/v3/geocode/regeo?key=06300d0f6db0ad587aa9676d6162ce6a&location=" + longitude + "," + latitude;
        HttpGet httpGet = new HttpGet(url);

        Future<HttpResponse> future = httpAsyncClient.execute(httpGet, null);

        // supplyAsync：消费者接口Supplier<T>，T get();
        CompletableFuture.supplyAsync(() -> {
            try {
                    HttpResponse response = future.get();
                    String province = null;
                    if (response.getStatusLine().getStatusCode() == 200) {
                        String res = EntityUtils.toString(response.getEntity());
                        //转成json对象
                        JSONObject jsonObject = JSONObject.parseObject(res);
                        JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                        if (regeocode != null && !regeocode.isEmpty()){
                            JSONObject address = regeocode.getJSONObject("addressComponent");
                            //获取省市区
                            province = address.getString("province");
                        }
                    }
                    return province;

            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }).thenAccept((String province) -> resultFuture.complete(Collections.singleton(ActivityBean.of(uid, aid, null, time, eventType, province))));

//        CompletableFuture.supplyAsync(new Supplier<String>() {
//            @Override
//            public String get() {
//                try {
//                    HttpResponse response = future.get();
//                    String province = null;
//                    if (response.getStatusLine().getStatusCode() == 200) {
//                        String res = EntityUtils.toString(response.getEntity());
//                        //转成json对象
//                        JSONObject jsonObject = JSONObject.parseObject(res);
//                        JSONObject regeocode = jsonObject.getJSONObject("regeocode");
//                        if (regeocode != null && !regeocode.isEmpty()){
//                            JSONObject address = regeocode.getJSONObject("addressComponent");
//                            //获取省市区
//                            province = address.getString("province");
//                        }
//                    }
//                    return province;
//                } catch (Exception e) {
//                    return null;
//                }
//            }
//        }).thenAccept((String province) ->
//                resultFuture.complete(Collections.singleton(ActivityBean.of(uid, aid, null, time, eventType, province)))
//        );
    }

    @Override
    public void close() throws Exception {
        super.close();
        httpAsyncClient.close();
    }
}
