package com.example.apitest.sinktest

import java.util.Properties

import com.example.apitest.SensorReading
import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object KafkaSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

//        val inputStream = env.readTextFile("E:\\Idea\\bigdata\\Flink2020\\src\\main\\resources\\sensor.txt")

        //从kafka读数据
        val pros = new Properties()
        pros.setProperty("bootstrap.servers", "hadoop102:9092")
        pros.setProperty("group.id", "consumer-group")
        pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        pros.setProperty("auto.offset.reset", "latest")

        val inputStream = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), pros))

        val dataStream = inputStream
            .map(data => {
                val dataArray = data.split(",")
                SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString
            })

        //落地CSV文件
//        dataStream.addSink(
//            StreamingFileSink.forRowFormat(
//                new Path("E:\\Idea\\bigdata\\Flink2020\\src\\main\\resources\\out.txt"),
//                new SimpleStringEncoder[String]("UTF-8")
//            ).build()
//        )

        //写入Kafka
        dataStream.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092", "sinkTest", new SimpleStringSchema()))

        env.execute("kafka sink test")
    }
}