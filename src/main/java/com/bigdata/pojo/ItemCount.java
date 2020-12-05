package com.bigdata.pojo;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 10:34
 */
public class ItemCount {

    public long itemId; //商品ID;
    public long windowEnd; //窗口结束时间戳
    public long buyCount; //购买数量

    public ItemCount() {
    }

    public ItemCount(long itemId, long windowEnd, long buyCount) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.buyCount = buyCount;
    }

    public static ItemCount of(long itemId, long windowEnd, long buyCount){
        return new ItemCount(itemId, windowEnd, buyCount);
    }
}
