package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;

public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        String filepath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filepath);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 传入事件时间:
        Time maxOutOfOrderness = Time.seconds(1); // 有界乱序的一个时间戳提取器
        dataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(maxOutOfOrderness) {
            @Override
            public long extractTimestamp(SensorReading sensorReading) {
                return sensorReading.getTimestamp()*1000L;
            }
        });

        // 如果事件本来就是升序的话直接按照升序数据设置事件时间和watermark
//        dataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//            @Override
//            public long extractAscendingTimestamp(SensorReading sensorReading) {
//                return sensorReading.getTimestamp()*1000L;
//            }
//        });

        // 基于事件时间的开窗聚合: 统计15秒内的温度最小值
        SingleOutputStreamOperator<SensorReading> Min_15s = dataStream.keyBy("id").
                timeWindow(Time.seconds(15)).minBy("temperature");

        env.execute();
    }
}
