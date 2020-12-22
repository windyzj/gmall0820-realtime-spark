package com.atguigu.gmall0820.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0820.publisher.service.DauService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

// @Controller   返回渲染好的页面
@RestController  //返回结果数据
public class PublisherController {

    // 依赖注入
    @Autowired
    DauService dauService;

    @RequestMapping("/hello/{name}")
    public  String  helloWorld(@RequestParam(value = "date",defaultValue = "2020-12-22") String dt, @PathVariable("name") String name){
        System.out.println(name+"||"+dt);
        ///
        Long dauTotal = dauService.getDauTotal(dt);

        return dauTotal+"";
    }

    @RequestMapping("realtime-total")
    public String realtimeTotal(@RequestParam(value = "date") String date){
        Long dauTotal = dauService.getDauTotal(date);


        String json ="[{\"id\":\"dau\",\"name\":\"新增日活\",\"value\":"+dauTotal+"},\n" +
                     "{\"id\":\"new_mid\",\"name\":\"新增设备\",\"value\":233} ]\n";

        return json;
    }
    @RequestMapping("realtime-hour")
    public String realtimeHour(@RequestParam("id")String id, @RequestParam("date") String date){
        if("dau".equals(id)){
            Map dauHourTdCount = dauService.getDauHourCount(date);
            String yd = toYd(date);
            Map dauHourYdCount = dauService.getDauHourCount(yd);

            Map rsMap=new HashMap();
            rsMap.put("today",dauHourTdCount);
            rsMap.put("yesterday",dauHourYdCount);
            return JSON.toJSONString(rsMap);
        }
       return  null;

    }

    private  String toYd(String td){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        try {
            Date tdDate = simpleDateFormat.parse(td);
            Date ydDate = DateUtils.addDays(tdDate, -1);
            String yd = simpleDateFormat.format(ydDate);
            return yd;

        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("日期格式转换异常");
        }


    }

}
