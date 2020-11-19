package com.examples.jedisdemo;

import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2020/11/15 21:09
 */
public class RedisTest {
    public static void main(String[] args) {
        //1、创建Jedis对象
        Jedis jedis = new Jedis("192.168.32.99", 6379);
        //2、操作
        Set<String> keys = jedis.keys("*");
        for (String key: keys) {
            //获取value
            String value = jedis.get(key);
            System.out.println(key+"========"+value);
        }
        //关掉jedis
        jedis.close();
    }
}
