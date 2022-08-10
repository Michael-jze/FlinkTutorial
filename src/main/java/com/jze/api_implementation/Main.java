package com.jze.api_implementation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        funct(executionEnvironment); // 运行函数
        executionEnvironment.execute();
    }
    private static void funct(StreamExecutionEnvironment env){
        DataStreamSource<Long> input = env.generateSequence(1, 100);
        input.print();
    }
}
