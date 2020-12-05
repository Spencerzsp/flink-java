package com.bigdata.flink.async_io;

import com.alibaba.druid.pool.DruidDataSource;
import com.bigdata.flink.conf.ConfigurationManager;
import com.bigdata.flink.constant.Constant;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * @ author spencer
 * @ date 2020/5/28 14:16
 * 异步查询mysql数据
 */
public class AsyncMysqlRequestFunction extends RichAsyncFunction<String, String> {

    //连接池
    private transient DruidDataSource dataSource;
    //线程池
//    private transient ExecutorService executorService;
    private transient ThreadPoolExecutor executorService;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        executorService = new ThreadPoolExecutor(
                5,
                20,
                3,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(3),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardOldestPolicy()
        );
        //初始化一个线程数量为20的线程池
//        executorService = Executors.newFixedThreadPool(20);
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(ConfigurationManager.config.getString(Constant.JDBC_DRIVER));
        dataSource.setUrl(ConfigurationManager.config.getString(Constant.JDBC_URL));
        dataSource.setUsername(ConfigurationManager.config.getString(Constant.JDBC_USER));
        dataSource.setPassword(ConfigurationManager.config.getString(Constant.JDBC_PASSWORD));
        dataSource.setInitialSize(5);
        dataSource.setMinIdle(10);
        dataSource.setMaxActive(20);
    }

    @Override
    public void asyncInvoke(String id, ResultFuture<String> resultFuture) throws Exception {

        //提交一个请求到线程池
        // executorService.submit():有返回值
        // executorService.execute():无返回值

        Future<String> future = executorService.submit(() -> queryFromMysql(id));

//        String s = future.get();

        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    return future.get();
                }catch (Exception e){
                    return null;
                }
            }
            // 接收前一个任务的结果，并消费处理，无返回值
            // resultFuture：Put all results in a {@link Collection} and then emit output.
        }).thenAccept(dbResult -> resultFuture.complete(Collections.singleton(dbResult)));
    }

    private String queryFromMysql(String param) throws SQLException {
        String sql = "select * from t_data where id = ?";
        String result = null;
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;

        try {
            connection = dataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, param);
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                //必须基于sql查询出来的数据字段
                String id = resultSet.getString("id");
                String name = resultSet.getString("name");
                String age = resultSet.getString("age");
                result = "id = " + id + ", " + "name = " + name + ", " + "age = " + age;
            }

        }finally {
            if (resultSet != null){
                resultSet.close();
            }
            if (preparedStatement != null){
                preparedStatement.close();
            }
            if (connection != null){
                connection.close();
            }
        }

        if (result != null){
            //放入缓存中
        }

        return result;
    }

    @Override
    public void close() throws Exception {
        super.close();

        dataSource.close();
//        threadPoolExecutor.shutdown();
        executorService.shutdown();
    }
}
