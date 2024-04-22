package com.zhisheng.flink.common;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CarLog {
  private String carCode;
  private String vin;
  private int speed;
  private long logTime;
  private String gposLongitude;
  private String gposLatitude;
}
