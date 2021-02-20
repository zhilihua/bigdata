package com.example.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 异步发生：带回调
 */
public class Producer2 {
    public static void main(String[] args) {
        //1、定义连接属性
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");//kafka集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2、实例化对象
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>(
                    "first", Integer.toString(i), Integer.toString(i)
            ), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("success->" + recordMetadata);
                    }else {
                        e.printStackTrace();
                    }
                }
            });
        }

        //3、关闭对象连接
        producer.close();
    }
}
