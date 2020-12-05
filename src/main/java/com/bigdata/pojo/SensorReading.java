package com.bigdata.pojo;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/8/5 13:26
 */
public class SensorReading {

    private String sensorId;

    private Long timestamp;

    private Double temperature;

    @Override
    public String toString() {
        return "SensorReading{" +
                "sensorId='" + sensorId + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                '}';
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }

    public SensorReading() {
    }

    public SensorReading(String sensorId, Long timestamp, Double temperature) {
        this.sensorId = sensorId;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public static SensorReading of(String sensorId, Long timestamp, Double temperature){
        return new SensorReading(sensorId, timestamp, temperature);
    }
}
