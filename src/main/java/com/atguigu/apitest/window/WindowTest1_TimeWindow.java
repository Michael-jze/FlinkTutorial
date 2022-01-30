package com.atguigu.apitest.window;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowTest1_TimeWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String filepath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataStreamSource<String> inputStream = env.readTextFile(filepath);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 窗口测试 -- 开窗测试
//        dataStream.windowAll() 这个方法是将所有的数据串行的传入同一个下游的算子中, 相当于是一个global -- 效率较低
        dataStream.keyBy("id");

        dataStream.keyBy("id").window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
        // 开启一个长度为15s的滚动窗口

        dataStream.keyBy("id").timeWindow(Time.seconds(15));
        // 这一个和上面的效果是一致的, 对于timeWindow接口传入一个参数是一个滚动窗口,timeWindow(Time size)
        // 其底层调用的是TumblingProcessingTimeWindows

        dataStream.keyBy("id").timeWindow(Time.seconds(15),Time.seconds(1));
        // 传入两个参数是一个滑动窗口 timeWindow(Time size,Time slide)
        // 其底层调用的是 SlidingProcessingTimeWindows

        dataStream.keyBy("id").window(EventTimeSessionWindows.withGap(Time.seconds(1)));
        // 事件时间Timeout为1s的会话窗口
        // 由于没有上层的sessionWindow的接口, 所以需要调用底层的窗口分类器

        dataStream.keyBy("id").window(ProcessingTimeSessionWindows.withGap(Time.seconds(1)));
        // 处理时间Timeout为1s的会话窗口
        // 这里有两个对于SessionWindow的实现

        dataStream.keyBy("id").countWindow(100); // 滚动计数窗口
        dataStream.keyBy("id").countWindow(100, 10); // 滑动计数窗口
        // 所有的计数窗口的底层都是使用的 GlobalWindows. 因为需要进行全局计数

        // 窗口分配器的分类:
        //  滚动窗口分配器, 滑动窗口分配器, 会话窗口分配器, 全局窗口分配器

        // 全局窗口分配器 GlobalWindows 可以设置 evictor(进行不需要的数据的移出), trigger(进行窗口的关闭)

        // ===== 窗口函数 -- 增量聚合函数:
        WindowedStream<SensorReading, Tuple, TimeWindow> windowedStream = dataStream.keyBy("id").
                window(TumblingProcessingTimeWindows.of(Time.seconds(15)));

        windowedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return sensorReading;
            }
        }); // 传入Reduce

        SingleOutputStreamOperator<Integer> resultStream = windowedStream.aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
            // 三个泛型分别是: 输入类型, 累加器类型, 输出类型
            @Override
            public Integer createAccumulator() { // 创建累加器的初始值
                return 0;
            } // 累加器初始化

            @Override
            public Integer add(SensorReading sensorReading, Integer integer) { // 当被输入一个对象会怎样进行操作
                return integer + 1;
            } // 累加器进行累加

            @Override
            public Integer getResult(Integer integer) { // 当窗口被关闭之后怎么进行输出
                return integer;
            } // 最终状态输出

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                // 一般是在sessionWindow中使用, 进行合并的操作, 一般是发生在同一个session被分配到了不同的分区
                return integer + acc1; // 这个函数实际上调用不到
            }
        });// AggregateFunction 的三个参数分别是 输入流类型, 累加器类型, 输出流类型. 这里实现的是一个counter

//        resultStream.print(); // 对于使用文件进行读取, 实际这里会没有输出, 因为这里进行的是时间分区, 而
        // 整个运行的时间小于一个时间分区. 所以最后一个窗口的输出没有被显示出来, 需要改成流式输入
//        DataStreamSource<String> inputStream = env.socketTextStream("localhost", 7777);
        // 由于这里是对不同的ID进行了分组, 所以, 这里实际显示的是对某个传感器在15s时间内获取的数据量
        // 之前的所有滚动聚合函数都是属于增量聚合函数

        // ===== 窗口函数 -- 全窗口函数:等待所有数据到齐之后进行计算然后输出
        // 典型的全窗口函数是: processWindowFunction 以及 WindowFunction 类型
        SingleOutputStreamOperator<Integer> resultStream2 = dataStream.keyBy("id").timeWindow(Time.seconds(1))
                .apply(new WindowFunction<SensorReading, Integer, Tuple, TimeWindow>() {
                    // 这里的四个参数分别是输入类型, 输出类型, 之前keyBy导致的分组类型, 以及最后的时间窗口信息
                    @Override
                    // 这里函数的形参分别为: 分类数据, 窗口数据, 通过迭代输入的输入数据, 通过搜集输出的输出数据
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Integer> collector) throws Exception {
                        Integer countSize = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(countSize);
                    }
                });
        resultStream2.print();

        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream3 = dataStream.keyBy("id").timeWindow(Time.seconds(1))
                .apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<SensorReading> iterable, Collector<Tuple3<String, Long, Integer>> collector) throws Exception {
                        String id = tuple.getField(0);
                        Long WindowEnd = timeWindow.getEnd();
                        Integer count = IteratorUtils.toList(iterable.iterator()).size();
                        collector.collect(new Tuple3<>(id, WindowEnd, count));
                    }
                });
        resultStream3.print();

        dataStream.keyBy("id").timeWindow(Time.seconds(1)).process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
            // 对于processWindowFunction其输入的四个参数分别为 输入类型, 输出类型, 分类类型, 窗口类型
            @Override
            public void process(
                    Tuple tuple,
                    ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>.Context context,
                    Iterable<SensorReading> iterable, Collector<Object> collector) throws Exception {

            }
        });

        // 其他可选API的使用范例:
        dataStream.keyBy("id").timeWindow(Time.seconds(15)).allowedLateness(Time.minutes(1));// 创建一个允许一分钟延迟的时间窗口


        env.execute();
    }
}
