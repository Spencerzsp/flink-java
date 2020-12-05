package com.bigdata.flink.join;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/3 13:48
 */
public class LeftSelectKey implements KeySelector<Tuple3<String, String, Long>, String> {
    @Override
    public String getKey(Tuple3<String, String, Long> value) throws Exception {
        return value.f0;
    }
}
