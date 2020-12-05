package com.bigdata.flink.sinks;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/11/26 17:23
 */
public class JDBCSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> numberDataStream = env.fromElements(1, 2, 3, 4, 5);
        numberDataStream.addSink(JdbcSink.sink(
                "INSERT INTO num_table values(?)",
                (ps, t) -> {
                    ps.setInt(1, t);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://wbbigdata01:3306/flink?useUnicode=true&characterEncoding=utf8")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("bigdata")
                        .build()
        ));

        env.execute("JDBCSinkDemo");
    }
}
