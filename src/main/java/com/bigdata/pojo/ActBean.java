package com.bigdata.pojo;

/**
 * @ author spencer
 * @ date 2020/6/11 13:14
 */
public class ActBean {

    public String uid;
    // 活动ID
    public String aid;
    public String time;
    // 事件类型
    public Integer type;
    public String province;
    public Integer count;

    @Override
    public String toString() {
        return "ActBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", time='" + time + '\'' +
                ", type=" + type +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }

    public ActBean() {
    }

    public ActBean(String uid, String aid, String time, Integer type, String province) {
        this.uid = uid;
        this.aid = aid;
        this.time = time;
        this.type = type;
        this.province = province;
    }

    public ActBean(String uid, String aid, String time, Integer type, String province, Integer count) {
        this.uid = uid;
        this.aid = aid;
        this.time = time;
        this.type = type;
        this.province = province;
        this.count = count;
    }

    public static ActBean of(String uid, String aid, String time, Integer type, String province){
        return new ActBean(uid, aid, time, type, province);
    }

    public static ActBean of(String uid, String aid, String time, Integer type, String province, Integer count){
        return new ActBean(uid, aid, time, type, province, count);
    }
}
