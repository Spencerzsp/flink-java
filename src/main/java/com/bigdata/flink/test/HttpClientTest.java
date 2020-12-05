package com.bigdata.flink.test;

import net.sf.json.JSON;
import net.sf.json.JSONObject;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * @ author spencer
 * @ date 2020/5/27 15:04
 */
public class HttpClientTest {

    public static void main(String[] args) throws Exception {
        double longitude = 121.26757;
        double latitude = 37.49794;

        String url = "https://restapi.amap.com/v3/geocode/regeo?key=06300d0f6db0ad587aa9676d6162ce6a&location=" + longitude + "," + latitude;

        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(url);
        CloseableHttpResponse response = httpClient.execute(httpGet);
        try {
            int status = response.getStatusLine().getStatusCode();
            String province = null;
            if (status == 200){
                //获取请求的json字符串
                String result = EntityUtils.toString(response.getEntity());
                System.out.println(result);

                //转成json对象
                JSONObject jsonObject = JSONObject.fromObject(result);
                JSONObject regeocode = jsonObject.getJSONObject("regeocode");
                if (regeocode != null && !regeocode.isEmpty()){
                    JSONObject address = regeocode.getJSONObject("addressComponent");
                    //获取省市区
                    province = address.getString("province");
                }
            }
            System.out.println(province);

        }finally {
            response.close();
        }
    }
}
