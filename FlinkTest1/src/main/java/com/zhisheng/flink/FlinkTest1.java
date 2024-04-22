package com.zhisheng.flink;

import com.zhisheng.flink.common.CarLog;
import com.zhisheng.flink.common.CarOverspeedFilter1;
import com.zhisheng.flink.common.CarOverspeedSink1;
import com.zhisheng.flink.common.CarSpeedSource1ps;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkTest1 {

    public static void main(String[] args) throws Exception{
        System.out.println("Hello world!");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CarSpeedSource1ps carSpeedSource1ps = new CarSpeedSource1ps();
        DataStreamSource<CarLog> source1=env.addSource(carSpeedSource1ps);
        DataStreamSource<CarLog> source2=env.addSource(carSpeedSource1ps);
        SingleOutputStreamOperator<CarLog> overSpeedStream1 = source1.filter(new CarOverspeedFilter1());
        overSpeedStream1.addSink(new CarOverspeedSink1());
        source1.print("source1 =>>>>");

        source2.print("source2 =>>>>");
        env.execute();
    }
}