package com.atguigu.fl_03_window_time.window;

import com.atguigu.bean.WaterSensor;
import com.atguigu.fl_02_datastream.ds_02_source.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Arrays;


public class Window {

    //按键分区窗口测试
    @Test
    public void test_01() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> lineDSS = env.socketTextStream("172.16.50.205", 7777);

        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> Arrays.stream(line.split(" ")).forEach(words::collect))
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS.sum(1);

        result.print();

        env.execute();
    }


    // TODO: 窗口分类
    @Test
    public void test_02(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("172.16.50.205", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());
        //基于时间的
        //滚动窗口
        sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //滑动窗口
        sensorKS.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)));
        //会话窗口
        sensorKS.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        //基于时间的
        //滚动窗口,5条数据一个窗口
        sensorKS.countWindow(5);
        //滑动窗口
        sensorKS.countWindow(5,2);
        //底层
        sensorKS.window(GlobalWindows.create());//全局窗口，需要自定义触发器，少用

    }

    // TODO: 窗口函数
    @Test
    public void test_03(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("172.16.50.205", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> sensorWindow = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        //增量聚合,来一条算一条
        //sensorWindow.reduce();
        //sensorWindow.aggregate();

        //全窗口函数,数据来了不计算,存起来,窗口触发的时候计算并输出结果
        //sensorWindow.process();

        //结合使用
    }

    @Test
    public void test_04() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("172.16.50.205", 7777)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> sensorKS = sensorDS.keyBy(sensor -> sensor.getId());

        WindowedStream<WaterSensor, String, TimeWindow> sensorWindow = sensorKS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)));
        //增量聚合,来一条算一条
        SingleOutputStreamOperator<WaterSensor> reduced = sensorWindow.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor t1, WaterSensor t2) throws Exception {
                        System.out.println("reduce: t1: " + t1 + " t2: "+t2);
                        return new WaterSensor(t1.getId(), t2.getTs(), t1.getVc() + t2.getVc());
                    }
                }
        );
        reduced.print();

        env.execute();
    }
}
