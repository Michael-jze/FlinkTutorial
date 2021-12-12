package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

// 这里是一个批处理wordCount程序
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 从文件中读取数据:
        String inputPath = "E:\\Java\\work_space\\FlinkTutorial\\src\\main\\resources\\hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        // 对数据集进行处理:
        // 思路: 将文本拆分为单个的单词, 并且构成 <String, int> 的元组, 第一个值是那个单词,
        // 第二个值都设置为1, 然后累加一个word下的所有元组的数值
        // 1. 按照空格分词展开, 转换为二元组
        FlatMapFunction<String, Tuple2<String, Integer>> mapFunction = new MyFlatMapFunction();
        FlatMapOperator<String, Tuple2<String, Integer>> operator = inputDataSet.flatMap(mapFunction);

        // 2. 对于数据集合按照单词进行分组, 然后计算;
        UnsortedGrouping<Tuple2<String, Integer>> unsortedGrouping = operator.groupBy(0);
        AggregateOperator<Tuple2<String, Integer>> aggregateOperator =  unsortedGrouping.sum(1);

        // 3. 获得结果
        DataSet<Tuple2<String,Integer>> resultSet = aggregateOperator;

        resultSet.print();

        /*
            (scala,1)
            (flink,1)
            (world,1)
            (hello,4)
            (spark,1)
        * */

    }

    // 自定义回调函数, 实现 FlatMapFunction 这个接口
    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // 按照空格分词
            String[] words = s.split(" ");
            // 遍历所有的单词, 包装为二元组然后输出
            for(String word : words){
                collector.collect(new Tuple2<>(word,1));
            }
        }
    }
}
