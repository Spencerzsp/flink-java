package com.bigdata.flink.test;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.SQLException;
import java.util.concurrent.*;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/11/26 14:14
 */
public class ThreadPoolTest {

    public static void main(String[] args) throws SQLException {

        ThreadPoolExecutor threadPool = createThreadPool();
        DruidDataSource druidDataSource = careteConnectionPool();
        DruidPooledConnection connection = druidDataSource.getConnection();
    }

    private static DruidDataSource careteConnectionPool() {
        DruidDataSource druidDataSource = new DruidDataSource();
        druidDataSource.setMaxActive(20);
        druidDataSource.setMinIdle(20);
        druidDataSource.setInitialSize(10);
        druidDataSource.setDriverClassName("com.mysql.jdbc.Driver");
        druidDataSource.setUrl("jdbc:mysql://wbbigdata01:3306/flink?useUnicode=true&characterEncoding=utf8&rewriteBatchedStatements=true");
        druidDataSource.setUsername("root");
        druidDataSource.setPassword("bigdata");

        return druidDataSource;
    }

    private static ThreadPoolExecutor createThreadPool() {
        int processors = Runtime.getRuntime().availableProcessors();
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
                2,
                processors,
                1000,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(3),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardOldestPolicy()
        );

        return threadPool;
    }


}
