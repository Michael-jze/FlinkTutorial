package com.atguigu.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest6_Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从文件读取数据
        String filepath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filepath);
        /*
        sensor_1,12345678,11.1
        sensor_2,23456789,22.2
        sensor_6,1234567890,66.6
        sensor_1,1234567801,11.2
        sensor_2,2345678901,22.3
        sensor_6,123456789001,66.4
        * */

        inputStream.print("input");

//        inputStream.shuffle().print("shuffle");

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });
//        dataStream.keyBy("id").print("keyby");

        inputStream.global().print("global");

        env.execute();
    }
}
