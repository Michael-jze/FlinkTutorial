package com.atguigu.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFormTest3_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");

        keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading storedState, SensorReading newValue) throws Exception {
                return new SensorReading(
                        storedState.getId(),
                        newValue.getTimestamp(),
                        Math.max(
                                storedState.getTemperature(),
                                newValue.getTemperature()
                        )
                );
            }
        });

        DataStream<SensorReading> resultStream = keyedStream.reduce((storedState, newValue) -> new SensorReading(
                storedState.getId(),
                newValue.getTimestamp(),
                Math.max(
                        storedState.getTemperature(),
                        newValue.getTemperature()
                )
        ));

        resultStream.print();
        env.execute();
    }
}
