package com.bigdata.flink.async_io;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * @ author spencer
 * @ date 2020/5/27 15:46
 * 使用HttpClient访问高德地图API，读取经纬度数据所对应的省份(同步阻塞的方式)
 */
public class LocationToActivityBeanFunction extends RichMapFunction<String, ActivityBean> {

    private CloseableHttpClient httpClient = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        httpClient = HttpClients.createDefault();
    }

    @Override
    public ActivityBean map(String line) throws Exception {
        String[] fields = line.split(",");
        String uid = fields[0];
        String aid = fields[1];
        String time = fields[2];
        int eventType = Integer.parseInt(fields[3]);
        double longitude = Double.parseDouble(fields[4]);
        double latitude = Double.parseDouble(fields[5]);

        String url = "https://restapi.amap.com/v3/geocode/regeo?key=06300d0f6db0ad587aa9676d6162ce6a&location=" + longitude + "," + latitude;
        String province = null;
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = httpClient.execute(httpGet);
        try {

            int status = response.getStatusLine().getStatusCode();

            if (status == 200){
                //获取请求的json字符串
                String result = EntityUtils.toString(response.getEntity());
                System.out.println(result);

                //转成json对象
                JSONObject jsonObject = JSONObject.parseObject(result);
                JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                if (regeocode != null && !regeocode.isEmpty()){
                    JSONObject address = regeocode.getJSONObject("addressComponent");
                    //获取省市区
                    province = address.getString("province");
                }
            }

        } finally {
            response.close();
        }

        return ActivityBean.of(uid, aid, null, time, eventType, longitude, latitude, province);
    }

    @Override
    public void close() throws Exception {
        super.close();
        httpClient.close();
    }
}
