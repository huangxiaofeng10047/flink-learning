package com.zhisheng.flink.common;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
public class CarOverspeedSink1 implements SinkFunction<CarLog>  {
    @Override
    public void invoke(CarLog value, Context context) throws Exception {
        System.out.println("CarOverspeedSink1: " + value);
    }

}
