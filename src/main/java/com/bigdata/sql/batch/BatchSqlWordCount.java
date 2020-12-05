package com.bigdata.sql.batch;

import com.bigdata.sql.bean.WordCount;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

/**
 * @description:
 * @author: spencer
 * @date: 2020/7/29 11:19
 */
public class BatchSqlWordCount {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<WordCount> input = env.fromElements(
                new WordCount("flink", 1L),
                new WordCount("spark", 1L),
                new WordCount("flink", 1L),
                new WordCount("spark", 1L),
                new WordCount("flink", 1L)

        );

        // 使用sql API
        tEnv.createTemporaryView("wordcount", input);
        Table table = tEnv.sqlQuery("select word, sum(counts) as counts from wordcount group by word");

        DataSet<WordCount> wordCountDataSet = tEnv.toDataSet(table, WordCount.class);

        // 使用table API
        Table table2 = tEnv.fromDataSet(input)
                .groupBy("word")
                .select("word, counts.sum as counts");

        DataSet<WordCount> countDataSet = tEnv.toDataSet(table2, WordCount.class);
        countDataSet.print();


        wordCountDataSet.print();
    }
}
