package com.bigdata.pojo;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 10:13
 */
public class UserAction {

    public long userId; //用户id
    public long itemId; //商品id
    public int categoryId; //商品分类id
    public String behavior; //用户行为（pv, buy, cart, fav)
    public long ts; //操作时间戳

    @Override
    public String toString() {
        return "UserAction{" +
                "userId=" + userId +
                ", itemId=" + itemId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timestamp=" + ts +
                '}';
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getItemId() {
        return itemId;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public int getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(int categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public long getTimestamp() {
        return ts;
    }

    public void setTimestamp(long timestamp) {
        this.ts = timestamp;
    }

    public UserAction() {
    }

    public UserAction(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.ts = timestamp;
    }

    public static UserAction of(long userId, long itemId, int categoryId, String behavior, long timestamp){
        return new UserAction(userId, itemId, categoryId, behavior, timestamp);
    }
}
