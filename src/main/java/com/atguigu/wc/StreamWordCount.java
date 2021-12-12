package com.atguigu.wc;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境:
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 读取文件: 从硬盘中读取数据只在测试环境中符合逻辑, 在生产环境一般来自Kafka
//        String inputPath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
//        DataStreamSource<String> dataStream = env.readTextFile(inputPath);

        // ubuntu子系统中开启服务: nc -lk 7777, 然后在命令行中输入字符
        // 当目标端口关闭连接关闭之后自动关闭数据流
//        DataStream<String> dataStream = env.socketTextStream("localhost",7777);

        // 使用parameter tool 工具从程序的启动参数中获取配置项
        // 在 Intellj中需要更改 Program arguments 为: --host localhost --port 7777 (Java 中传递参数使用两个横杠)
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port  = parameterTool.getInt("port");
        DataStream<String> dataStream = env.socketTextStream(host,port);

        // 基于数据流进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMapRes = dataStream.flatMap(new WordCount.MyFlatMapFunction());
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByRes = flatMapRes.keyBy(0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumRes = keyByRes.sum(1);
        // 这里的是在定义运行的步骤, 而且这里 DataStreamSource, SingleOutputStreamOperator, KeyedStream 都是DataStream的子类

        sumRes.print();

        env.execute();
        /* 前面的数值是并行进程的编号, 可以看到同一个key下的任务被安排到同一个进程中进行,
        由于print和sum绑定, 所以每个进程不止一次进行输出, 只要执行了一个sum就进行了一个输出
        为什么这里到达了七? 因为电脑是一个八核电脑, 在env中有设置, 进程数量默认等于计算机核数
            5> (world,1)
            3> (hello,1)
            7> (flink,1)
            1> (spark,1)
            3> (hello,2)
            3> (hello,3)
            3> (hello,4)
            1> (scala,1)

        env.setParallelism(2); 之后:
            1> (hello,1)
            2> (world,1)
            1> (spark,1)
            2> (flink,1)
            1> (hello,2)
            1> (scala,1)
            1> (hello,3)
            1> (hello,4)

        env.setParallelism(2); 之后Flink会省略输出前面的进程标识
            (hello,1)
            (world,1)
            (hello,2)
            (flink,1)
            (hello,3)
            (spark,1)
            (hello,4)
            (scala,1)
        * */
    }
}
