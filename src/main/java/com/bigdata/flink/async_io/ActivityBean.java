package com.bigdata.flink.async_io;

/**
 * @ author spencer
 * @ date 2020/5/27 13:49
 * 封装数据信息
 */
public class ActivityBean {

    public String uid;

    public String aid;

    public String activityName;

    public String time;

    public int eventType;

    public double longitude;

    public double latitude;

    public String province;

    public int count = 1;

    @Override
    public String toString() {
        return "ActivityBean{" +
                "uid='" + uid + '\'' +
                ", aid='" + aid + '\'' +
                ", activityName='" + activityName + '\'' +
                ", time='" + time + '\'' +
                ", eventType=" + eventType +
                ", longitude=" + longitude +
                ", latitude=" + latitude +
                ", province='" + province + '\'' +
                ", count=" + count +
                '}';
    }

    public ActivityBean() {
    }

    public ActivityBean(String uid, String aid, String activityName, String time, int eventType, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.time = time;
        this.eventType = eventType;
        this.province = province;
    }

    public ActivityBean(String uid, String aid, String activityName, String time, int eventType, double longitude, double latitude, String province) {
        this.uid = uid;
        this.aid = aid;
        this.activityName = activityName;
        this.time = time;
        this.eventType = eventType;
        this.longitude = longitude;
        this.latitude = latitude;
        this.province = province;
    }

    public static ActivityBean of(String uid, String aid, String activityName, String time, int eventType, String province){

        return new ActivityBean(uid, aid, activityName, time, eventType, province);
    }

    public static ActivityBean of(String uid, String aid, String activityName, String time, int eventType, double longitude, double latitude, String province){

        return new ActivityBean(uid, aid, activityName, time, eventType, longitude, latitude, province);
    }
}
