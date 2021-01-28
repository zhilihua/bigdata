package com.example.dw.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2021/1/28 22:42
 */
public class MyInterceptor implements Interceptor{

    //创建一个放置复合要求数据的集合
    private List<Event> results=new ArrayList<>();

    private String startFlag="\"en\":\"start\"";

    @Override
    public void initialize() {

    }

    //核心方法，拦截Event
    @Override
    public Event intercept(Event event) {

        byte[] body = event.getBody();

        //在header中添加key
        Map<String, String> headers = event.getHeaders();

        String bodyStr = new String(body, Charset.forName("utf-8"));

        boolean flag=true;

        //符合启动日志特征
        if (bodyStr.contains(startFlag)) {

            headers.put("topic", "topic_start");

            flag=ETLUtil.validStartLog(bodyStr);

        }else {

            //事件日志
            headers.put("topic", "topic_event");

            flag=ETLUtil.validEventLog(bodyStr);

        }

        //如果验证结果是false
        if (!flag) {
            return null;
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {

        //先清空results
        results.clear();

        for (Event event : events) {

            Event result = intercept(event);

            //有可能intercept(event)，event不符合要求，会拦截掉，返回null
            if (result !=null) {

                //放入合法的数据集合中
                results.add(result);

            }

        }

        return results;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        //从flume的配置文件中读取参数
        @Override
        public void configure(Context context) {


        }

        //创建一个拦截器对象
        @Override
        public Interceptor build() {
            return new MyInterceptor();
        }


    }

}

