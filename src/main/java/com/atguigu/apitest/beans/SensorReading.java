package com.atguigu.apitest.beans;

// 传感器温度读数类型:
public class SensorReading {
    private String id;
    private Long timestamp;
    private Double temperature;

    public SensorReading(){

    }

    public SensorReading(String id, Long timestamp, Double temperature){
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }


}
