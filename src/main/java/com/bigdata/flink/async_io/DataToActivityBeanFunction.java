package com.bigdata.flink.async_io;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @ author spencer
 * @ date 2020/5/27 13:56
 */
public class DataToActivityBeanFunction extends RichMapFunction<String, ActivityBean>{

    private Connection connection = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //创建MySQL连接
        connection = DriverManager.getConnection("jdbc:mysql://wbbigdata01:3306/flink?useUnicode=true&characterEncoding=utf8", "root", "bigdata");

    }

    @Override
    public ActivityBean map(String line) throws Exception {
        String[] fields = line.split(",");
        String uid = fields[0];
        String aid = fields[1];
        PreparedStatement pstmt = null;
        String name = null;

        try {
            //根据aid作为查询条件，查询出activityName
            pstmt = connection.prepareStatement("SELECT name from t_activity WHERE id = ?");
            pstmt.setString(1, aid);
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()){
                name = resultSet.getString(1);
            }
        } finally {
            if (pstmt != null){
                pstmt.close();
            }
        }

        String time = fields[2];
        int eventType = Integer.parseInt(fields[3]);
        String province = fields[4];

        return ActivityBean.of(uid, aid, name,time, eventType, province);
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
