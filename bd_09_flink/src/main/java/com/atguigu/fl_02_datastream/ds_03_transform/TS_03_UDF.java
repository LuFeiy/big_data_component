package com.atguigu.fl_02_datastream.ds_03_transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class TS_03_UDF {

    @Test
    public void test_01() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        //实现接口：省略

        //匿名内部类
        /*SingleOutputStreamOperator<WaterSensor> filter = stream.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return waterSensor.id.equals("sensor_1");
            }
        });*/

        //lambda表达式
        SingleOutputStreamOperator<WaterSensor> filter = stream.filter(sensor -> sensor.id.equals("sensor_1"));

        filter.print();

        env.execute();

    }


    //富函数类
    @Test
    public void test_02() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env
                .fromElements(1,2,3,4)
                .map(new RichMapFunction<Integer, Integer>() {
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期开始");
                    }

                    @Override
                    public Integer map(Integer integer) throws Exception {
                        return integer + 1;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                        System.out.println("索引是：" + getRuntimeContext().getIndexOfThisSubtask() + " 的任务的生命周期结束");
                    }
                })
                .print();




        env.execute();
    }
}
