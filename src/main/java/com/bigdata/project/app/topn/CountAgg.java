package com.bigdata.project.app.topn;

import com.bigdata.pojo.UserAction;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 10:46
 */
public class CountAgg implements AggregateFunction<UserAction, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserAction value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
