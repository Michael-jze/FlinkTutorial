package com.atguigu.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SinkTest2_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        String filepath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filepath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost").setPort(6379).build();
        dataStream.addSink(new RedisSink<>(config, new RedisMapper<SensorReading>() {
            @Override
            public RedisCommandDescription getCommandDescription() {
                // 定义保存数据到Redis的命令, 这里存储称为Hash, hset sensor_tmp id temp
                return new RedisCommandDescription(RedisCommand.HSET, "sensor_tmp");
            }

            @Override
            public String getKeyFromData(SensorReading sensorReading) {
                return sensorReading.getId();
            }

            @Override
            public String getValueFromData(SensorReading sensorReading) {
                return sensorReading.getTemperature().toString();
            }
        }));

        env.execute();
    }
}
