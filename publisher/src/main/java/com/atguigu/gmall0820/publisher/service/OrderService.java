package com.atguigu.gmall0820.publisher.service;

import java.util.Map;

public interface OrderService {

    public Map searchOrderWide(String date,String keyword,int pageNo,int pagesize);
}
