package com.atguigu.transform;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformTest2_RollingAggregation {
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

        // map成一个SensorReading类型:
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
//            }
//        });

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 分组: 对于类, 不能通过位置进行keyby. 基于位置的keyby只能用于tuple上, 需要基于字段名称进行实现, 这里需要getter支持
        KeyedStream<SensorReading, Tuple> keyByStream = dataStream.keyBy("id");
            // 这里可以传递多个Field进行组合分组
        KeyedStream<SensorReading, String> keyByThroughSelectorStream = dataStream.keyBy(data -> data.getId());
            // 通过选择器进行获取

        // 滚动聚合: 取最大温度
        DataStream<SensorReading> resultStream = keyByStream.max("temperature");
//        resultStream.print();
        /*
        SensorReading{id='sensor_1', timestamp=12345678, temperature=11.1}
        SensorReading{id='sensor_2', timestamp=23456789, temperature=22.2}
        SensorReading{id='sensor_6', timestamp=1234567890, temperature=66.6}
        SensorReading{id='sensor_1', timestamp=12345678, temperature=11.2}
        SensorReading{id='sensor_2', timestamp=23456789, temperature=22.3}
        SensorReading{id='sensor_6', timestamp=1234567890, temperature=66.6} -- 只更新了温度数据,但是没有更新其他数据
        * */
        DataStream<SensorReading> resultStream1 = keyByStream.maxBy("temperature");
        resultStream1.print();
        /*
        SensorReading{id='sensor_1', timestamp=12345678, temperature=11.1}
        SensorReading{id='sensor_2', timestamp=23456789, temperature=22.2}
        SensorReading{id='sensor_6', timestamp=1234567890, temperature=66.6}
        SensorReading{id='sensor_1', timestamp=1234567801, temperature=11.2} --> 发生了对于时间字段的更新
        SensorReading{id='sensor_2', timestamp=2345678901, temperature=22.3}
        SensorReading{id='sensor_6', timestamp=1234567890, temperature=66.6}
        * */
        env.execute();
    }
}
