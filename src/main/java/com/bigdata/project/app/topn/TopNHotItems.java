package com.bigdata.project.app.topn;

import com.bigdata.pojo.ItemCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 11:01
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemCount, String> {

    private int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    private ListState<ItemCount> itemState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<ItemCount> listStateDescriptor = new ListStateDescriptor<>("item-count", ItemCount.class);
        itemState = getRuntimeContext().getListState(listStateDescriptor);
    }

    @Override
    public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
        itemState.add(value);
        ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        List<ItemCount> allItems = new ArrayList<>();
        Iterable<ItemCount> itemCounts = itemState.get();
        for (ItemCount itemCount : itemCounts) {
            allItems.add(itemCount);
        }

        itemState.clear();
        // 按照点击量从大到小排序
        allItems.sort(new Comparator<ItemCount>() {
            @Override
            public int compare(ItemCount o1, ItemCount o2) {
                return (int) (o2.buyCount - o1.buyCount);
            }
        });

        //将排名信息格式化成String，方便打印
        StringBuilder result = new StringBuilder();
        result.append("========================================\n");
        result.append("时间：").append(new Timestamp(timestamp-1)).append("\n");
        for (int i=0;i<topSize;i++) {
            ItemCount currentItem = allItems.get(i);
            // No1:  商品ID=12224  购买量=2
            result.append("No").append(i).append(":")
                    .append("  商品ID=").append(currentItem.itemId)
                    .append("  购买量=").append(currentItem.buyCount)
                    .append("\n");
        }

        result.append("====================================\n");
        out.collect(result.toString());
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
