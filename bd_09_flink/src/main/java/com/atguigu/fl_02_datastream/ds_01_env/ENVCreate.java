package com.atguigu.fl_02_datastream.ds_01_env;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Arrays;

public class ENVCreate {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        // 自动判断
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //本地执行
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //远程执行,实际测试不成功
        /*StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("hadoop102",
                6123,
                "D:\\DataScience\\Code\\big_data_component\\bd_09_flink\\target\\bd_09_flink-1.0-SNAPSHOT.jar");
                */




        // 2. 读取文本流
        DataStreamSource<String> lineDSS = env.socketTextStream("hadoop102", 7777);
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> Arrays.stream(line.split(" ")).forEach(words::collect))
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        // 6. 打印
        result.print();

        // 7. 触发执行
        env.execute();

    }

    @Test
    public void test_01() throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取文本流
        DataStreamSource<String> lineDSS = env.socketTextStream("hadoop102", 7777);
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> Arrays.stream(line.split(" ")).forEach(words::collect))
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        // 6. 打印
        result.print();

        // 7. 执行
        env.execute();
    }
}
