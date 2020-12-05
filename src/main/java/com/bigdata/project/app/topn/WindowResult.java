package com.bigdata.project.app.topn;

import com.bigdata.pojo.ItemCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 10:47
 */
public class WindowResult extends ProcessWindowFunction<Long, ItemCount, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, java.lang.Iterable<Long> elements, Collector<ItemCount> out) throws Exception {
        Long itemId = tuple.getField(0);

//        Long itemId = ((Tuple1<Long>) tuple).f0;
        Long count = elements.iterator().next();
        long windowEnd = context.window().getEnd();

        out.collect(ItemCount.of(itemId, windowEnd, count));
    }
}
