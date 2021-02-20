package com.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 异步生产:不带回调函数
 */
public class Producer1 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //1、定义连接属性
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");    //kafka集群
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //2、实例化对象
        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i=0; i<100; i++){
            producer.send(new ProducerRecord<String, String>(
                  "first",
                  Integer.toString(i),
                  Integer.toString(i)
            )).get();    //带上get为同步发送，否则为异步发送
        }

        //3、关闭对象连接
        producer.close();
    }
}
