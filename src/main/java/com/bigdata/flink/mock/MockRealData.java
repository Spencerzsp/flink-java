package com.bigdata.flink.mock;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @ author spencer
 * @ date 2020/6/5 14:26
 */
public class MockRealData {

    public static void main(String[] args) {

        String brokers = "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092";
        String topics = "flink_test";
        KafkaProducer<String, String> kafkaProducer = createKafkaProducer(brokers);

        while (true){

            for (String data : generateData()) {
                kafkaProducer.send(new ProducerRecord<String, String>(topics, data));
                System.out.println(data);
            }
        }
    }

    private static List<String> generateData() {

        List<String> list = new ArrayList<>();
        Random random = new Random();

        List<String> cityList = new ArrayList<>();

        List<String> jobList = new ArrayList<>();

        cityList.add("北京");
        cityList.add("上海");
        cityList.add("广州");
        cityList.add("深圳");
        cityList.add("成都");
        cityList.add("杭州");

        jobList.add("大数据开发");
        jobList.add("java开发");
        jobList.add("人工智能开发");
        jobList.add("高级前端");
        jobList.add("数据科学家");

        for (int i = 0; i < 100; i++) {

            long timestamp = System.currentTimeMillis();
            String city = cityList.get(random.nextInt(cityList.size()));
            String job = jobList.get(random.nextInt(jobList.size()));
            int adid = random.nextInt(50);
            int userid = random.nextInt(100);

            String info = timestamp + " " + job + " " + city + " " + userid + " " + adid;

            list.add(info);
        }

        return list;
    }

    private static KafkaProducer<String, String> createKafkaProducer(String brokers) {

        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(prop);

        return kafkaProducer;
    }
}
