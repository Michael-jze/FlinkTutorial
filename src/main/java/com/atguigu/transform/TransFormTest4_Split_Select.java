package com.atguigu.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

public class TransFormTest4_Split_Select {
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
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 按照温度是否大于30度分为两条流
        SplitStream<SensorReading> splitStream = dataStream.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) { // 可以按照多种标签进行打标, 进行多维度的综合分流
                return (sensorReading.getTemperature() > 30) ?
                        Collections.singleton("high") :
                        Collections.singleton("low");
            }
        });
        DataStream<SensorReading> highStream = splitStream.select("high");// 输入一个标签组, 满足标签组中的任何一个标签的数据会被提取
//        highStream.print();
        /*
        SensorReading{id='sensor_6', timestamp=1234567890, temperature=66.6}
        SensorReading{id='sensor_6', timestamp=123456789001, temperature=66.4}
        * */
        DataStream<SensorReading> lowStream = splitStream.select("low");
//        lowStream.print();
        /*
        SensorReading{id='sensor_1', timestamp=12345678, temperature=11.1}
        SensorReading{id='sensor_2', timestamp=23456789, temperature=22.2}
        SensorReading{id='sensor_1', timestamp=1234567801, temperature=11.2}
        SensorReading{id='sensor_2', timestamp=2345678901, temperature=22.3}
        * */
        DataStream<SensorReading> allStream = splitStream.select("low","high");
//        allStream.print();
        /*
        SensorReading{id='sensor_1', timestamp=12345678, temperature=11.1}
        SensorReading{id='sensor_2', timestamp=23456789, temperature=22.2}
        SensorReading{id='sensor_6', timestamp=1234567890, temperature=66.6}
        SensorReading{id='sensor_1', timestamp=1234567801, temperature=11.2}
        SensorReading{id='sensor_2', timestamp=2345678901, temperature=22.3}
        SensorReading{id='sensor_6', timestamp=123456789001, temperature=66.4}
        * */

        //###合流: 将highStream转化为Tuple<SensorId, Temperature>的格式, 然后和没有被转化的lowStream进行合并, 输出一个状态信息
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });

        // Connect - Map合流
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowStream);

        SingleOutputStreamOperator<Tuple3<String, Double, String>> jointStream = connectedStreams.map(
                new CoMapFunction<Tuple2<String, Double>, SensorReading, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "warning message!");
            }

            @Override
            public Tuple3<String, Double, String> map2(SensorReading sensorReading) throws Exception {
                return new Tuple3<>(sensorReading.getId(), sensorReading.getTemperature(), "normal");
            }
        });
//        jointStream.print();
        /*
        (sensor_6,66.6,warning message!)
        (sensor_1,11.1,normal)
        (sensor_6,66.4,warning message!)
        (sensor_2,22.2,normal)
        (sensor_1,11.2,normal)
        (sensor_2,22.3,normal)
        * */

        // Union合流:
        SingleOutputStreamOperator<Tuple3<String, Double, String>> tmpStream = warningStream.map(new MapFunction<Tuple2<String, Double>, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                return new Tuple3<>(stringDoubleTuple2.f0, stringDoubleTuple2.f1, "warning message!");
            }
        });

        SingleOutputStreamOperator<Tuple3<String, Double, String>> tmpStream1 = lowStream.map(new MapFunction<SensorReading, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map(SensorReading sensorReading) throws Exception {
                return new Tuple3<>(sensorReading.getId(), sensorReading.getTemperature(), "normal");
            }
        });
        DataStream<Tuple3<String, Double, String>> finalStream = tmpStream.union(tmpStream1);
        finalStream.print();
        /*
        (sensor_6,66.6,warning message!)
        (sensor_6,66.4,warning message!)
        (sensor_1,11.1,normal)
        (sensor_2,22.2,normal)
        (sensor_1,11.2,normal)
        (sensor_2,22.3,normal)
        * */
        env.execute();
    }
}
