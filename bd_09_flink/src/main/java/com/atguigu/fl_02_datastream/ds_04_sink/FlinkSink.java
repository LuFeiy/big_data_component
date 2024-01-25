package com.atguigu.fl_02_datastream.ds_04_sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;

public class FlinkSink {

    @Test
    public void test_01_toFile() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 每个目录中，都有 并行度个数的 文件在写入
        env.setParallelism(2);

        // 必须开启checkpoint，否则一直都是 .inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);


        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long value) throws Exception {
                        return "Number:" + value;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000),
                Types.STRING
        );

        DataStreamSource<String> dataGen = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        // 输出到文件系统
        FileSink<String> fieSink = FileSink
                // 输出行式存储的文件，指定路径、指定编码
                .<String>forRowFormat(new Path("output"), new SimpleStringEncoder<>("UTF-8"))
                // 输出文件的一些配置： 文件名的前缀、后缀
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("atguigu-")
                                .withPartSuffix(".log")
                                .build()
                )
                // 按照目录分桶：如下，就是每个小时一个目录
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault()))
                // 文件滚动策略:  1分钟 或 1m
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(1))
                                .withMaxPartSize(new MemorySize(1024*1024))
                                .build()
                )
                .build();


        dataGen.sinkTo(fieSink);

        env.execute();

    }

    //未测试成功
    @Test
    public void test_02_toHDFS() throws Exception {
       //flink写入数据到hdfs
         //1.创建流处理环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            //2.设置并行度
            env.setParallelism(1);
            //3.设置检查点
            env.enableCheckpointing(5000);
            //4.读取数据
            DataStreamSource<String> stream = env.socketTextStream("hadoop102", 7777);
            //5.写入数据

            FileSink<String> hdfsSink = HdfsSink.getHdfsSink();
            stream.sinkTo(hdfsSink);
            //6.执行任务
            env.execute();
    }
}
