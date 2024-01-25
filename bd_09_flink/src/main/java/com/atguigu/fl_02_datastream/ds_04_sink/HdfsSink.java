package com.atguigu.fl_02_datastream.ds_04_sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author ：wudl
 * @date ：Created in 2021-12-26 23:49
 * @description：HdfsSink
 * @modified By：
 * @version: 1.0
 */

public class HdfsSink {
    public static FileSink<String> getHdfsSink() {
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("yuan")
                .withPartSuffix(".txt")
                .build();
        FileSink fileSink = FileSink.forRowFormat(new Path("hdfs://192.168.1.102:8020/tmp/flink"), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMinutes(1))
                                //每隔5分钟没有新数据到来,也把之前的生成一个新文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMinutes(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .withOutputFileConfig(config)
                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd"))
                .build();

        return  fileSink;
    }
}
