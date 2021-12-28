package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class WindowTest2_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filepath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filepath);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        SingleOutputStreamOperator<Double> averageStream = dataStream.keyBy("id").countWindow(10, 2) // 开一个十个数据的窗口, 然后以步长为2进行滑动
                .aggregate(new AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>() {
                    @Override
                    public Tuple2<Double, Integer> createAccumulator() {
                        return new Tuple2<Double, Integer>(0.0, 0);
                    }

                    @Override
                    public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
                        return new Tuple2<>(accumulator.f0 + value.getTemperature(), accumulator.f1 + 1);
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Integer> accumulator) {
                        return accumulator.f0 / accumulator.f1;
                    }

                    @Override
                    public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
                        return null;
                    }
                });
        averageStream.print();
        // 这里和时间窗口非常不一样, 在计数窗口创建的时候会将空余部分自动补齐:
        // 即使你刚输入两个数, 由于其自动补齐了前面的八个空位, 所以同样也会输出.
        // 这种现象是数据流开始时发生的不稳定行为, 当数据流长度大于窗口长度之后变为稳定输出

        env.execute();
    }
}
