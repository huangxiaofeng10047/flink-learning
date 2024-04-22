package com.zhisheng.flink.common;

import org.apache.flink.api.common.functions.FilterFunction;

public class CarOverspeedFilter1 implements FilterFunction<CarLog> {

    @Override
    public boolean filter(CarLog carLog) throws Exception {
        return carLog.getSpeed() > 100 ? true : false;
    }
}
