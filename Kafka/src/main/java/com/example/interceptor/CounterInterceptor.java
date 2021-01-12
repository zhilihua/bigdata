package com.example.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String> {
    private int errorCounter = 0;
    private int successCounter = 0;
    /**
     * 自定义Record：修改原始的recoder
     * @param record： 旧的recoder
     * @return： 新的recoder
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    /**
     * 收到ack后调用
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // 统计成功和失败的次数
        if (e == null) {
            successCounter++;
        } else {
            errorCounter++;
        }
    }

    /**
     * 关闭Producer时候调用
     */
    @Override
    public void close() {
        // 保存结果
        System.out.println("Successful sent: " + successCounter);
        System.out.println("Failed sent: " + errorCounter);
    }

    /**
     * 定义拦截器的方法
     * @param map
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
