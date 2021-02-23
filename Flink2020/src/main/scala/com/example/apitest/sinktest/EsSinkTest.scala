package com.example.apitest.sinktest

import java.util

import com.example.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("E:\\Idea\\bigdata\\Flink2020\\src\\main\\resources\\sensor.txt")
        val dataStream = inputStream
            .map(data => {
                val dataArray = data.split(",")
                SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
            })

        // 定义一个httpHost
        val httpHosts = new util.ArrayList[HttpHost]()
        httpHosts.add(new HttpHost("hadoop102", 9200))
        // 定义一个ElasticsearchSinkFunction
        val esSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
            override def process(t: SensorReading, ctx: RuntimeContext, indexer: RequestIndexer): Unit = {
                // 包装写入es的数据
                val dataSource = new util.HashMap[String, String]()
                dataSource.put("sensor_id", t.id)
                dataSource.put("temp", t.temperature.toString)
                dataSource.put("ts", t.timestamp.toString)
                //创建一个index request
                val indexRequest = Requests.indexRequest()
                    .index("sensor_temp")
                    .`type`("readingdata")
                    .source(dataSource)
                //用indexer发送 http请求
                indexer.add(indexRequest)
            }
        }

        dataStream.addSink( new ElasticsearchSink.Builder[SensorReading](httpHosts, esSinkFunc).build() )

        env.execute("es sink test")
    }
}
