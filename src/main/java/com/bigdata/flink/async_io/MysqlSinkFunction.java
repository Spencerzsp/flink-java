package com.bigdata.flink.async_io;

import com.bigdata.flink.conf.ConfigurationManager;
import com.bigdata.flink.constant.Constant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @ author spencer
 * @ date 2020/5/29 13:39
 *
 */
public class MysqlSinkFunction extends RichSinkFunction<ActivityBean> {

    private transient Connection connection = null;

    /**
     * 初始化连接配置
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        String url = ConfigurationManager.config.getString(Constant.JDBC_URL);
        String user = ConfigurationManager.config.getString(Constant.JDBC_USER);
        String password = ConfigurationManager.config.getString(Constant.JDBC_PASSWORD);
        //创建mysql连接
        connection = DriverManager.getConnection(url, user, password);

    }

    @Override
    public void invoke(ActivityBean activityBean, Context context) throws Exception {

        PreparedStatement psmt = null;
        try {
            //mysql实现upsert操作，插入数据时若存在主键冲突则更新，若无冲突则插入
            String sql = "insert into t_activity_count(aid, event_type, counts) values (?, ?, ?) on duplicate key update counts = ?";
            psmt = connection.prepareStatement(sql);
            psmt.setString(1, activityBean.aid);
            psmt.setInt(2, activityBean.eventType);
            psmt.setInt(3, activityBean.count);
            psmt.setInt(4, activityBean.count);

            psmt.executeUpdate();
        } finally {
            if (psmt != null){
                psmt.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
