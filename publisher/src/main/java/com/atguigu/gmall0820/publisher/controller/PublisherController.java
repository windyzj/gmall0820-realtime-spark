package com.atguigu.gmall0820.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall0820.publisher.bean.Option;
import com.atguigu.gmall0820.publisher.bean.Stat;
import com.atguigu.gmall0820.publisher.service.DauService;
import com.atguigu.gmall0820.publisher.service.OrderService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

// @Controller   返回渲染好的页面
@RestController  //返回结果数据
public class PublisherController {

    // 依赖注入
    @Autowired
    DauService dauService;

    @Autowired
    OrderService orderService;

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

    @RequestMapping("sale_detail")
    public String getSaleDetail(@RequestParam("date")String date,
                                @RequestParam("startpage") int startPage,
                                @RequestParam("size") int size,
                                @RequestParam("keyword") String keyword){

        Map resultMap = orderService.searchOrderWide(date, keyword, startPage, size);
        Long total = (Long)resultMap.get("total");
        List detail = (List)resultMap.get("detail");
        Map ageAggMap = (Map)resultMap.get("ageAggMap");

        Long agelt20=0L;
        Long ageGte20lt30=0L;
        Long ageGte30=0L;
        for (Object o: ageAggMap.entrySet()) {
            Map.Entry<String,Long> entry=( Map.Entry<String,Long>)o;
            String ageStr = entry.getKey();
            Long ageCt = entry.getValue();
            Integer age = Integer.valueOf(ageStr);
            if(age<20){
                agelt20+=ageCt;
            }else if(age>=30){
                ageGte30+=ageCt;
            }else{
                ageGte20lt30+=ageCt;
            }
        }
        List ageOptions=new ArrayList();
        Double agelt20rate=  Math.round( agelt20*1000D/total)/10D;
        Double ageGte20lt30rate=Math.round( ageGte20lt30*1000D/total)/10D;;
        Double ageGte30rate=Math.round( ageGte30*1000D/total)/10D;;
        ageOptions.add(new Option("20岁以下",agelt20rate));
        ageOptions.add(new Option("20岁到30岁",ageGte20lt30rate));
        ageOptions.add(new Option("30岁及以上",ageGte30rate));


        //把年龄-->年龄段
        //把年龄段的个数 变成百分比

       //  性别
        Map genderAggMap = (Map)resultMap.get("genderAggMap");
         Long  maleCt = (Long)genderAggMap.get("M");
         Long  femaleCt = (Long)genderAggMap.get("F");
         Double maleRate=  Math.round( maleCt*1000D/total)/10D;
         Double femaleRate=  Math.round( femaleCt*1000D/total)/10D;
        List genderOptions=new ArrayList();
        genderOptions.add(new Option("男",maleRate));
        genderOptions.add(new Option("女",femaleRate));

        List<Stat> statList=new ArrayList<>();
        statList.add(new Stat(genderOptions,"性别占比"));
        statList.add(new Stat(ageOptions,"年龄占比"));

        Map  finalMap = new HashMap<>();
        finalMap.put("total",total);
        finalMap.put("stat",statList);
        finalMap.put("detail",detail);
        return  JSON.toJSONString(finalMap);

    }




}
