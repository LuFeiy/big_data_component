package com.atguigu.fl_02_datastream.ds_02_source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SourceTest {

    @Test
    public void test_01() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Integer> data = Arrays.asList(1, 22, 3);

        DataStreamSource<Integer> ds = env.fromCollection(data);

        ds.print();

        env.execute();
    }


    @Test
    public void test_02() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(),new Path("input\\word.txt")).build();

        DataStreamSource<String> ds = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file");

        ds.print();

        env.execute();
    }

    @Test
    public void test_03() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lineDSS = env.socketTextStream("hadoop102", 7777);

        lineDSS.print();

        env.execute();
    }

    @Test
    public void test_04() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                                        .setBootstrapServers("172.16.50.200:9090")
                                        .setTopics("TOPIC_DATA_ACCOUNT_HOLDINGS_RECALCULATION_REPORT")
                                        .setGroupId("flink")
                                        .setStartingOffsets(OffsetsInitializer.earliest())
                                        .setValueOnlyDeserializer(new SimpleStringSchema())
                                        .build();

        DataStreamSource<String> stream = env.fromSource(kafkaSource,WatermarkStrategy.noWatermarks(),"kafka-source");

        stream.print("kafka");

        env.execute();
    }


    @Test
    public void test_05() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "number:" + value;
                    }
                }
                ,Long.MAX_VALUE
                ,RateLimiterStrategy.perSecond(10)
                ,Types.STRING
        );

        env.fromSource(dataGeneratorSource,WatermarkStrategy.noWatermarks(),"data-generator").print();

        env.execute();
    }
}
