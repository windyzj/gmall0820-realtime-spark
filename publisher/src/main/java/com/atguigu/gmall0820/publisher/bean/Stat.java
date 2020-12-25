package com.atguigu.gmall0820.publisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@Data
public class Stat {
    List<Option>  options;
    String title;
}
