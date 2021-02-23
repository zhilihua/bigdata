package com.example.apitest.sinktest

import com.example.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //读取数据
        val inputStream = env.readTextFile("E:\\Idea\\bigdata\\Flink2020\\src\\main\\resources\\sensor.txt")

        //数据处理
        val dataStream = inputStream
            .map(data => {
                val dataArray = data.split(",")
                SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
            })

        //写入redis
        val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build()

        dataStream.addSink( new RedisSink[SensorReading](conf, new MyRedisMapper))
        env.execute("redis sink test")
    }
}

class MyRedisMapper extends RedisMapper[SensorReading] {
    // 定义要执行的redis命令
    override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
    }

    //获取redis的key
    override def getKeyFromData(t: SensorReading): String = t.id

    //获取redis的value
    override def getValueFromData(t: SensorReading): String = t.temperature.toString
}