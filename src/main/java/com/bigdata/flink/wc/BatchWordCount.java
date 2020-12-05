package com.bigdata.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/6 14:52
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 使用类加载器读取配置文件
        URL resource = BatchWordCount.class.getClassLoader().getResource("words.txt");

        DataSet<String> lines = env.readTextFile(resource.getPath());

        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : line.split(" ")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOne.groupBy(0).sum(1);

        sum.print();

    }

}
