package com.atguigu.fl_02_datastream.ds_03_transform;

import com.atguigu.bean.Order;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TS_01_BasicTransform {

    @Test
    public void test_01_map() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        );

        //1. 传入MapFunction的实现类
        //stream.map(new UserMap()).print();

        //2. 使用匿名类
        /*stream.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor waterSensor) throws Exception {
                return waterSensor.id;
            }
        }).print();*/

        //3. 使用lambda表达式
        stream.map(waterSensor -> waterSensor.id).print();

        env.execute();

    }


    @Test
    public void test_02_filter() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        //使用匿名类
        /*stream.filter(new FilterFunction<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId().equals("sensor_1");
            }
        }).print();*/

        //使用lambda
        stream.filter(waterSensor -> waterSensor.getId().equals("sensor_1")).print();

        env.execute();
    }


    //flatmap的本质是一对多
    @Test
    public void test_03_flatMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 2L, 2),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        //使用匿名类
        /*stream.flatMap(new FlatMapFunction<WaterSensor, String >() {
            @Override
            public void flatMap(WaterSensor waterSensor, Collector<String> out) throws Exception {
                out.collect(String.valueOf(waterSensor.getTs()));
                out.collect(String.valueOf(waterSensor.getVc()));
                out.collect(String.valueOf(waterSensor.getId()));
            }
        }).print();*/


        stream.flatMap((WaterSensor waterSensor, Collector<String> out) -> {
            out.collect(String.valueOf(waterSensor.getTs()));
            out.collect(String.valueOf(waterSensor.getVc()));
            out.collect(String.valueOf(waterSensor.getId()));
        }).returns(Types.STRING).print();



        env.execute();
    }


    @Test
    public void test_04_keyBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 11L, 11),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        //使用Lambda表达式
        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(e -> e.id);

        //简单聚合
        //max,min
        //keyedStream.max("ts").print();



        //sum
        //keyedStream.sum("ts").print();

        //minby 整条数据
        keyedStream.maxBy("ts").print();
        //keyedStream.min("ts").print();

        //

        env.execute();
    }


    @Test
    public void test_04_reduce() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_1", 11L, 11),
                new WaterSensor("sensor_2", 2L, 2),
                new WaterSensor("sensor_3", 3L, 3)
        );

        KeyedStream<WaterSensor, String> keyedStream = stream.keyBy(e -> e.id);

        SingleOutputStreamOperator<WaterSensor> reduced = keyedStream.reduce((ws1, ws2) -> {
            return new WaterSensor(ws1.id, ws2.ts, ws1.vc + ws2.vc);
        });

        reduced.print();

        env.execute();
    }


    @Test
    public void test_05_SplitStreamByOutputTag(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //SingleOutputStreamOperator<WaterSensor> ds = env.socketTextStream("hadoop102", 7777)

    }


    @Test
    public void test_06() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 生成订单数据流
        List<Order> orders = new ArrayList<>();
        orders.add(new Order("1", "user1", "merchant1", 100.0));
        orders.add(new Order("2", "user2", "merchant2", 200.0));
        orders.add(new Order("3", "user1", "merchant2", 300.0));
        orders.add(new Order("4", "user2", "merchant2", 400.0));
        DataStream<Order> orderStream = env.fromCollection(orders);


        // 使用KeyBy算子将数据流按照用户ID和商家ID分区，并对每个用户和商家的订单金额求和
        KeyedStream<Order, String> keyedStream = orderStream.keyBy(Order::getUserId);
        SingleOutputStreamOperator<Order> maxAmount = keyedStream.max("amount");
        //maxAmount.print();


        //使用KeyBy算子将数据流按照用户ID和商家ID分区，并对每个用户和商家的订单金额求和
        //会报错
        //KeyedStream<Order, Tuple2<String, String>> orderTuple2KeyedStream = orderStream.keyBy((KeySelector<Order, Tuple2<String, String>>) order -> Tuple2.of(order.getUserId(), order.getMerchantId()));

        KeyedStream<Order, Tuple2<String, String>> orderTuple2KeyedStream = orderStream.keyBy(new KeySelector<Order, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Order order) throws Exception {
                return Tuple2.of(order.getUserId(), order.getMerchantId());
            }
        });

        //SingleOutputStreamOperator<Order> amount = orderTuple2KeyedStream.sum("amount");
        //amount.print();


        SingleOutputStreamOperator<Order> amount1 = orderTuple2KeyedStream.maxBy("amount");
        amount1.print();

        env.execute();
    }

}







