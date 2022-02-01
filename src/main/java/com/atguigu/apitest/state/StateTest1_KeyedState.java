package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StateTest1_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filepath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filepath);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作:统计当前sensor的数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.keyBy("id").map(new MyCountMapper());

//        resultStream.print();

        SingleOutputStreamOperator<Double> resultStream2 = dataStream.keyBy("id").flatMap(new MyTempIncreaseWarning(10.0));

        env.execute();
    }

    public static class MyCountMapper extends RichMapFunction<SensorReading, Integer>{
        private ValueState<Integer> keyCountState;
        // 不能直接在这里调用, 因为getRuntimeContext需要在开始运行后才能获取, 而在此处声明意味着在main中实例化的时候就进行调用

        // 其他类型键控状态的声明:
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override // 当运行开始之后进行初始化的函数
        public void open(Configuration parameters) throws Exception{
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class,0));
            myListState = getRuntimeContext().getListState(new ListStateDescriptor<String>("my-list-state", String.class));
            myMapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Double>("my-map-state", String.class, Double.class));
//            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>("my-reducing-state", /*reducing funcion*/, SensorReading.class))
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            for(String s:myListState.get()) {
                System.out.print(s);
            }
            myListState.add("hello");

            myMapState.get("1");
            myMapState.put("2",1.0);

            Integer tmp = keyCountState.value() + 1;
            keyCountState.update(tmp);
            return tmp;
        }
    }

    public static class MyTempIncreaseWarning extends RichFlatMapFunction<SensorReading, Double>{
        private final Double threshold;

        private ValueState<Double> previousTempState;

        public MyTempIncreaseWarning(Double threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration conf) throws Exception{
            previousTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("previous", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Double> out) throws Exception {
            Double lastTemp = previousTempState.value();
            if (lastTemp != null){
                double diff = Math.abs(value.getTemperature() - lastTemp);
                if (diff >= threshold) out.collect(value.getTemperature());
            }
            previousTempState.update(value.getTemperature());
        }
    }
}
