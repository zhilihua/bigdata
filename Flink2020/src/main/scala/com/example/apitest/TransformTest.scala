package com.example.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala._

object TransformTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStreamFromFile = env.readTextFile("E:\\Idea\\bigdata\\Flink2020\\src\\main\\resources\\sensor.txt")

        // 1.基本转换操作
        val dataStream = inputStreamFromFile
            .map(data => {
                val dataArray = data.split(",")
                SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
            })

        // 2.分组滚到聚合
        val aggStream = dataStream
            .keyBy("id")
            //            .keyBy( data => data.id)
            //            .keyBy(new MyIDSelector)
//            .min("temperature")     // 取当前sensor的最小温度值
//            .reduce(new MyReduce)
            .reduce( (curTmp, newTmp) => {
                SensorReading(curTmp.id, curTmp.timestamp.max(newTmp.timestamp),
                    curTmp.temperature.min(newTmp.temperature))
            })   // 聚合出每个sensor的最大时间戳和最小温度值

        // 3.分流
        val splitStream = dataStream
            .split(data => {
                if (data.temperature > 30)
                    Seq("high")
                else
                    Seq("low")
            })
        val highTempStream = splitStream.select("high")
        val lowTempStream = splitStream.select("low")
        val allTempStream = splitStream.select("high", "low")

        // 4.合流
        val warningStream: DataStream[(String, Double)] = highTempStream.map(
            data => (data.id, data.temperature)
        )
        val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)
        val resultStream: DataStream[Product] = connectedStreams.map(
            warningData => (warningData._1, warningData._2, "high temp warn"),
            lowTempData => (lowTempData.id, "normal")
        )

        val unionStream = highTempStream.union(lowTempStream, allTempStream)

        // 打印
        resultStream.print("result")
        env.execute("transform test job")
    }
}

// 自定义函数类，key选择器
class MyIDSelector() extends KeySelector[SensorReading, String] {
    override def getKey(in: SensorReading): String = in.id
}

// 自定义函数类 ReduceFunction
class MyReduce extends ReduceFunction[SensorReading] {
    override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
        SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temperature.min(t1.temperature))
    }
}