package com.zhisheng.flink;
import com.zhisheng.flink.common.CarLog;
import com.zhisheng.flink.common.CarSpeedSource1ps;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

public class FlinkTest2 {
    public static void main(String[] args) throws Exception{
        System.out.println("Hello world2!");

        //environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CarSpeedSource1ps carSpeedSource1ps=new CarSpeedSource1ps() ;
        DataStreamSource source1=env.addSource(carSpeedSource1ps);
        DataStreamSource source2=env.addSource(carSpeedSource1ps);
        DataStream source3=source1.union(source2);
        source3.print("source3 =>>>");
         SingleOutputStreamOperator<Object> source4= source3
             .keyBy(new KeySelector<CarLog, Object>() {
                      @Override
                      public String getKey(CarLog value) throws Exception {
                            return value.getCarCode();
                      }
                 }).flatMap(new RichFlatMapFunction<CarLog,Object>() {
                 ValueState<Integer> valueState;

                 @Override
                 public void open(Configuration parameters) throws Exception {
                     super.open(parameters);
                     ValueStateDescriptor<Integer> overSpeedCount= new ValueStateDescriptor<Integer>("overSpeedCount",
                         TypeInformation.of(new TypeHint<>() {
                             @Override
                             public TypeInformation<Integer> getTypeInfo() {
                                 return super.getTypeInfo();
                             }
                         })
                         );
                        valueState=getRuntimeContext().getState(overSpeedCount);

                 }

                 @Override
                 public void flatMap(CarLog carLog, Collector<Object> collector) throws Exception {
                      Integer value = valueState.value();

                      if (null == value) {
                          value = Integer.valueOf(0);
                      }
                        if (carLog.getSpeed() > 100) {
                            value++;
                            valueState.update(value);
                        }
                     collector.collect(Tuple2.of(carLog.getCarCode(), value));
                 }
             });
         source4.print("source4 =>>>");
         env.execute();
    }
}
