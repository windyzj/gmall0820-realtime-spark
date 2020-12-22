package com.atguigu.gmall0820.publisher.service;

import java.util.Map;

public interface DauService {

    public  Long getDauTotal(String date);

    public Map getDauHourCount(String date);
}
