package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class SourceTest1_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<SensorReading> dataStream = env.fromCollection(Arrays.asList(
                new SensorReading("name_1",1L,1.0),
                new SensorReading("name_2",1L,1.0),
                new SensorReading("name_3",1L,1.0)
        ));
        DataStreamSource<Integer> integerDataStream = env.fromElements(1, 2, 4, 56, 7);

        dataStream.print("data_stream_name");
        integerDataStream.print("int_stream_name");

        env.execute("jobName");
    }
}
