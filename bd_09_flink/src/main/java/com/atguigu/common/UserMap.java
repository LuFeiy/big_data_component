package com.atguigu.common;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class UserMap implements MapFunction<WaterSensor,String> {
    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.getId();
    }
}
