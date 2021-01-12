package com.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.*;
import java.time.Duration;
import java.util.*;

/**
 * 自定义保存offset
 */
public class ConsumerManual {
    private static Map<TopicPartition, Long> offset = new HashMap<>();
    private static String file = "d:/offset";
    public static void main(String[] args) {
        //参数配置，实例化对象
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");//Kafka集群
        props.put("group.id", "test");//消费者组，只要group.id相同，就属于同一个消费者组
        props.put("enable.auto.commit", "false");//关闭自动提交offset
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //订阅主题，拉取消息
        consumer.subscribe(Collections.singletonList("first"),
                new ConsumerRebalanceListener() {  //进行分区分配
                    //分区分配前做的事
                    @Override
                    public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                        //提交旧的offset
                        commit();
                    }
                    //分区分配后做的事
                    @Override
                    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                        //获取新的offset
                        readOffset(partitions);
                        for (TopicPartition partition: partitions) {
                            //读取内部缓存中的offset
                            Long os = offset.get(partition);
                            //让消费者定位到指定地点进行消费
                            if(os == null){
                                consumer.seek(partition, 0);   //从0开始消费
                            }else {
                                consumer.seek(partition, os);
                            }
                        }
                    }
                }
        );
        
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
            //原子绑定
            {
                for (ConsumerRecord<String, String> record : records) {
                    //消费
                    System.out.println(record);
                    offset.put(new TopicPartition(record.topic(), record.partition()),
                            record.offset());   //将消费位置进行缓存
                    commit();
                }
            }
        }
    }

    //提交该消费者所有分区的offset
    private static void commit() {
        //1、先从文件中读取旧的offset
        ObjectInputStream objectInputStream = null;
        Map<TopicPartition, Long> temp;
        try {
            objectInputStream = new ObjectInputStream(new FileInputStream(file));
            temp = (Map<TopicPartition, Long>) objectInputStream.readObject();
        } catch (Exception e) {
            temp = new HashMap<TopicPartition, Long>();
        }finally {
            if(objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //2、合并我们的offset
        temp.putAll(offset);

        //3、将新的offset写出去
        ObjectOutputStream objectOutputStream=null;
        try {
            objectOutputStream = new ObjectOutputStream(new FileOutputStream(file));
            objectOutputStream.writeObject(temp);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (objectOutputStream != null) {
                try {
                    objectOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //获取某分区的最新offset
    private static void readOffset(Collection<TopicPartition> partitions) {
        ObjectInputStream objectInputStream = null;
        Map<TopicPartition, Long> temp;
        try {
            objectInputStream = new ObjectInputStream(new FileInputStream(file));
            temp = (Map<TopicPartition, Long>) objectInputStream.readObject();
        } catch (Exception e) {
            temp = new HashMap<TopicPartition, Long>();
        }finally {
            if(objectInputStream != null) {
                try {
                    objectInputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //从全部分区中读取我们分配到的分区offset
        for (TopicPartition partition : partitions) {
            //将文件中的offset读取到offset缓存
            offset.put(partition, temp.get(partition));
        }
    }
}
