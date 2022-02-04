package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateTest4_CheckPoint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 状态后端配置:
        env.setStateBackend(new FsStateBackend(""));
        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new RocksDBStateBackend(""));

        // 检查点的配置:
        env.enableCheckpointing(10); // 输入一个检查点的时间间隔, long ms 时间戳, 在时间戳之后可以设置检查点模式
        env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 可以通过不同的方式进行配置的设置
        env.getCheckpointConfig().setCheckpointTimeout(1000); // 设置超时时间, 如果状态后端被存满或者出现故障一般表现为保存超时,
        // 防止由于状态后端出现故障导致整个应用程序宕机 (因为当多流检查点生成时会出现等待的情况)
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2); // 有可能前一个checkpoint没有保存完就进行了下一个checkpoint,
        // 防止发生状态后端的性能浪费
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10); //前一个checkpoint保存完成到下一个checkpoint开始保存之间的时间
        // 如果设置这一项, 相当于设置 setMaxConcurrentCheckpoints(1), enableCheckpointing(10 + 检查点生成时间)
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true); // 倾向于使用检查点而不是(savepoint + checkpoint)进行恢复
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10); // 容忍checkpoint失败的次数

        // 重启策略的配置
        env.setRestartStrategy(RestartStrategies.fallBackRestart()); // 回滚策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
        // 每隔1秒钟进行一个重启, 重启10次, 如果全部失败就认为任务失败
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(10), Time.minutes(1)));
        // 在十分钟内每隔一分钟共重启三次, 如果失败则任务任务失败


        String filepath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filepath);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

//        dataStream.process(new CoProcessFunction<>())


        env.execute();
    }
}
