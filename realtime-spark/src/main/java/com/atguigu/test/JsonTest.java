package com.atguigu.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

public class JsonTest {

    public static void main(String[] args) {
        // 对象转json字符串
        Movie movie = new Movie();
        movie.id="101";
        movie.name="沐浴之王";
        movie.doubanScore=6.5D;
        Directors director = new Directors();
        director.id="222";
        director.name="易小星";
        movie.directors=director;

        ArrayList<Actor> actorList = new ArrayList<>();
        actorList.add( new Actor("999","彭昱畅"));
        actorList.add( new Actor("888","乔杉"));
        movie.setActorList(actorList);

        //对象转字符串  // fastjson  gson  jackson
        String jsonstr = JSON.toJSONString(movie);
        System.out.println(jsonstr);

        Movie movie1 = JSON.parseObject(jsonstr, Movie.class);
        System.out.println(movie1);
        //JsonObject
        JSONObject jsonObject = JSON.parseObject(jsonstr);
        JSONArray actorList1 = jsonObject.getJSONArray("actorList");
        JSONObject qiaoshanActor = actorList1.getJSONObject(1);
        String qiaoshanName = qiaoshanActor.getString("name");
        System.out.println(qiaoshanName);

    }

    @Data
    public static class  Movie {
        String id;
        String name;
        Double doubanScore;
        Directors directors;
        List<Actor> actorList;
    }

   @Data
   @AllArgsConstructor
   public static class Actor{
        String id;
        String name;
    }

    @Data
    public static class Directors{
        String id;
        String name;
    }

}
