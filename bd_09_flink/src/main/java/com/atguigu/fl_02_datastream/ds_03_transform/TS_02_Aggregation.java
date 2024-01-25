package com.atguigu.fl_02_datastream.ds_03_transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class TS_02_Aggregation {


    @Test
    public void test_01_keyBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        //使用lambda表达式
        //KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(WaterSensor->WaterSensor.getId());
        //KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(WaterSensor::getId);

        //匿名类
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        });

        keyedStream.print();

        env.execute();
    }


    @Test
    public void test_02() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 10),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(WaterSensor::getId);

        //SingleOutputStreamOperator<WaterSensor> vcMax = keyedStream.max("vc");
        //vcMax.print();

        //SingleOutputStreamOperator<WaterSensor> vcMin = keyedStream.min("vc");
        //vcMin.print();


        //只对vc字段累计，其他字段保留第一条的值,max和min也是一样的
        SingleOutputStreamOperator<WaterSensor> vcSum = keyedStream.sum("vc");
        vcSum.print();

        env.execute();
    }


    @Test
    public void test_03() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(WaterSensor::getId);

        //会保留最值的整条数据
        //SingleOutputStreamOperator<WaterSensor> vcMaxBy = keyedStream.maxBy("vc");
        //vcMaxBy.print();

        SingleOutputStreamOperator<WaterSensor> vcMinBy = keyedStream.minBy("vc");
        vcMinBy.print();

        env.execute();
    }


    @Test
    public void test_04_reduce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(WaterSensor::getId);


        /*
        //reduce实现sum
        SingleOutputStreamOperator<WaterSensor> reduceSum = keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                return new WaterSensor(waterSensor.getId(), waterSensor.getTs(), waterSensor.getVc() + t1.getVc());
            }
        });
        reduceSum.print();
        */


        /*
        //reduce实现Max
        SingleOutputStreamOperator<WaterSensor> reduceMax = keyedStream.reduce((ws1, ws2) -> new WaterSensor(ws1.getId(),
                ws1.getTs(),
                ws1.getVc() > ws2.getVc() ? ws1.getVc() : ws2.getVc()
        ));
        reduceMax.print();
        */

        //reduce实现Min
        SingleOutputStreamOperator<WaterSensor> reduceMaxBy = keyedStream.reduce((ws1, ws2) -> new WaterSensor(ws1.getId(),
                ws2.getTs(),
                ws1.getVc() > ws2.getVc() ? ws1.getVc() : ws2.getVc()
        ));
        reduceMaxBy.print();

        env.execute();
    }
}
