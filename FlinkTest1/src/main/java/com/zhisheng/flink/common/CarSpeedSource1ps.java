package com.zhisheng.flink.common;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CarSpeedSource1ps implements SourceFunction<CarLog>{

    private boolean needRun= true;
    @Override
    public void run(SourceContext<CarLog> sourceContext) throws Exception {
        Random random = new Random();
        CarLog carLog = new CarLog();
        carLog.setCarCode("car_" + random.nextInt(5));

        long logTime= 0;
        int speed=0;
        while (needRun) {
            logTime = System.currentTimeMillis()-50  - random.nextInt(500);
            speed = random.nextInt(150);
            carLog.setSpeed(speed);
            carLog.setLogTime(logTime);

            sourceContext.collect(carLog);
            Thread.sleep(1000);

        }
    }

    @Override
    public void cancel() {
       needRun = false;
    }
}
