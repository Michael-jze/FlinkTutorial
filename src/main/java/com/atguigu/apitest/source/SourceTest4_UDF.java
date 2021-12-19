package com.atguigu.apitest.source;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

public class SourceTest4_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<SensorReading> streamSource = env.addSource(
                new MySensorSource());

        streamSource.print();
        env.execute();
    }

    public static class MySensorSource implements SourceFunction<SensorReading> {
        // 定义一个标志位, 看是否产生数据
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {
            // 随机数发生器:
            Random random = new Random();
            // 设置10个传感器的初始温度:
            HashMap<String, Double> senserTempMap = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                senserTempMap.put("sensor_"+(i+1), random.nextGaussian() * 20 + 60);
            }

            while (running){
                for (String sensorId : senserTempMap.keySet()){
                    Double newTemp = senserTempMap.get(sensorId) + random.nextGaussian();
                    senserTempMap.put(sensorId,newTemp);
                    sourceContext.collect(new SensorReading(sensorId, System.currentTimeMillis(), newTemp));

                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
