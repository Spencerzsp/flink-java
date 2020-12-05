package com.bigdata.flink.mysql;

import com.bigdata.flink.bean.User;
import com.bigdata.flink.test.TestApp;
import com.bigdata.pojo.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/9/27 13:18
 */
public class JDBCFormatDemo {

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(TestApp.class);

        logger.info("this is logger!");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        DataSource<SensorReading> dataSource1 = env.readCsvFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\sensor.txt")
//                .pojoType(SensorReading.class, "sensorId", "timestamp", "temperature");

        DataSource<String> lines = env.readTextFile("D:\\IdeaProjects\\flink-java\\src\\main\\resources\\sensor.txt");
        MapOperator<String, Row> mapDataSource = lines.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String value) throws Exception {
                String[] fields = value.split(",");
                String sensorId = fields[0];

                // 将Long类型转换为Timestamp的类型，并指定格式
                long time = Long.parseLong(fields[1]);
                Date date = new Date(time);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Timestamp timestamp = Timestamp.valueOf(sdf.format(date));

                double temperature = Double.parseDouble(fields[2]);
                Row row = new Row(3);
                row.setField(0, sensorId);
                row.setField(1, timestamp);
                row.setField(2, temperature);
                return row;
            }
        });

        // 准备使用JDBCOutputFormat写入数据到mysql,类似spark写入mysql
        mapDataSource.output(
                JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://wbbigdata01:3306/test")
                .setUsername("root")
                .setPassword("bigdata")
                .setQuery("insert into sensor values (?, ?, ?)")
                .setSqlTypes(new int[]{Types.VARCHAR, Types.TIMESTAMP, Types.DOUBLE})
                .finish()
        );

        // 读取mysql中的数据
        DataSource<Row> rowDataSource = env.createInput(
                JDBCInputFormat.buildJDBCInputFormat()
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setDBUrl("jdbc:mysql://wbbigdata01:3306/test")
                        .setUsername("root")
                        .setPassword("bigdata")
                        .setQuery("select * from user")
                        .setRowTypeInfo(new RowTypeInfo(
                                BasicTypeInfo.INT_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.INT_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO))
                        .finish()
        );

        // 将Row对象转换为具体对应的java的对象
        MapOperator<Row, User> userDataSource = rowDataSource.map(new MapFunction<Row, User>() {
            @Override
            public User map(Row value) throws Exception {
                Integer id = (Integer) value.getField(0);
                String name = (String) value.getField(1);
                Integer age = (Integer) value.getField(2);
                String addr = (String) value.getField(3);
                return User.of(id, name, age, addr);
            }
        });

        userDataSource.print();

    }
}
