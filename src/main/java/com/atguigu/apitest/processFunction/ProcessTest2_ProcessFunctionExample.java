package com.atguigu.apitest.processFunction;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ProcessTest2_ProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filepath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filepath);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 检测温度在10s中连续上升
        dataStream.keyBy("id").process(new TmpContIncrease(10)).print();

        // 进行侧输出流的实现
        OutputTag<SensorReading> lowTempTag = new OutputTag<>("lowTemp");
        SingleOutputStreamOperator<SensorReading> highTmpStream = dataStream.process(new ProcessFunction<SensorReading, SensorReading>() {
            @Override
            public void processElement(SensorReading sensorReading, ProcessFunction<SensorReading, SensorReading>.Context context, Collector<SensorReading> collector) throws Exception {
                if (sensorReading.getTemperature() > 30){
                    collector.collect(sensorReading);
                } else {
                    context.output(lowTempTag, sensorReading);
                }
            }
        });

        DataStream<SensorReading> lowTempStream = highTmpStream.getSideOutput(lowTempTag);

        env.execute();
    }

    public static class TmpContIncrease extends KeyedProcessFunction<Tuple, SensorReading, String>{
        private Integer interval;
        public TmpContIncrease(Integer interval){
            this.interval = interval;
        }

        private ValueState<Double> lastTimeTmp;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            lastTimeTmp = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTmp", Double.class));
            lastTimeTmp.update(Double.MIN_VALUE);
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTsStatw", Long.class));
            timerTsState.update(Long.MIN_VALUE);
        }

        @Override
        public void processElement(SensorReading sensorReading, KeyedProcessFunction<Tuple, SensorReading, String>.Context context, Collector<String> collector) throws Exception {
            Double lastTmp = lastTimeTmp.value();
            Long timerTs = timerTsState.value();

            //如果温度上升且没有定时器的时候注册定时器:
            if (sensorReading.getTemperature() > lastTmp && timerTs == Long.MIN_VALUE){
                Long ts = context.timerService().currentProcessingTime() + interval * 1000;
                context.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);
            } else if (sensorReading.getTemperature() < lastTmp && timerTs != Long.MIN_VALUE){
                context.timerService().deleteEventTimeTimer(timerTs);
                timerTsState.update(Long.MIN_VALUE);
            }
            lastTimeTmp.update(sensorReading.getTemperature());

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorReading, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            timerTsState.update(Long.MIN_VALUE);
            lastTimeTmp.update(Double.MIN_VALUE);
            out.collect("sensor"+ctx.getCurrentKey().getField(0)+" temperature "+ interval + " increase.");

        }
    }
}
