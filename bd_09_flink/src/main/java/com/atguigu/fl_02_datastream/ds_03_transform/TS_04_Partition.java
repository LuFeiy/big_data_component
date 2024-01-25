package com.atguigu.fl_02_datastream.ds_03_transform;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public class TS_04_Partition {

    //业务场景：在实时数据流处理中，您需要对传感器数据进行序列化处理。首先，将传感器数据转换为 JSON 格式，然后在后续步骤中提取特定的字段进行处理。
    @Test
    public void test_01_forward() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        // 假设 SensorData 是一个传感器数据类
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("172.16.50.200:9090")
                .setTopics("TOPIC_DATA_ACCOUNT_HOLDINGS_RECALCULATION_REPORT")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafka-source");



        // 提取特定字段
        DataStream<String> extractedStream = stream
                .forward() // 使用 forward 分区策略
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String json) throws Exception {
                        return extractFieldFromJson(json, "accountKey");
                    }
                });

        extractedStream.print();

        env.execute();
    }

    private static String extractFieldFromJson(String json, String filed) {
        //从json中获取accountKey
        // 将JSON字符串转换为JSONArray对象
        JSONArray jsonArray = new JSONArray(json);

        // 获取第一个JSONObject
        JSONObject jsonObject = jsonArray.getJSONObject(0);

        // 从JSONObject中获取field字段的值
        String fieldValue = String.valueOf(jsonObject.getInt(filed));

        return fieldValue;
    }

    @Test
    public void test_01_shuffled() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 7777);;

        DataStream<String> shuffled = stream.shuffle();

        //查看数据属于哪个subtask
        stream.print();

        shuffled.print();

        env.execute();

    }


    @Test
    public void test_02_rebalance() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 7777);

        stream.print();
        stream.rebalance().print();

        env.execute();

    }


    @Test
    public void test_03_rescale() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 7777);;

        stream.rescale().print();

        env.execute();

    }

    @Test
    public void test_04() throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 构建数据流
        DataStream<Integer> input = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 对数据流进行重新分区
        DataStream<Integer> rescaledStream = input.rescale();

        // 对重新分区后的数据流进行map操作
        DataStream<Integer> mappedStream = rescaledStream.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });

        // 对map后的数据流进行filter操作
        DataStream<Integer> filteredStream = mappedStream.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;
            }
        });

        // 打印分区后的数据流
        filteredStream.print();

        // 执行任务
        env.execute();
    }
}
