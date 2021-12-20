package com.atguigu.sink;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class SinkTest3_ES {
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

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("localhost",9200));
        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new ElasticsearchSinkFunction<SensorReading>() {
            @Override
            public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                // 定义写入的数据源
                HashMap<String, String> dataSource = new HashMap<>();
                dataSource.put("id",sensorReading.getId());
                dataSource.put("temp",sensorReading.getTemperature().toString());
                dataSource.put("ts",sensorReading.getTimestamp().toString());

                // 创建请求作为向es发起的写入命令
                IndexRequest indexRequest = Requests.indexRequest().index("sensor").type("readingdata").source(dataSource);
                // 使用indexer进行数据的发送
                requestIndexer.add(indexRequest);

            }
        }).build());

        env.execute();
    }
}
