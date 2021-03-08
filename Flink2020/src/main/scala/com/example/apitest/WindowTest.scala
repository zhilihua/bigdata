package com.example.apitest

import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2021/2/25 16:48
 */
object WindowTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)   //指定窗口时间类型
        env.getConfig.setAutoWatermarkInterval(100L)   //设置水位线生成周期

        val inputStream = env.readTextFile("D:\\IDEACode\\bigdata\\Flink2020\\src\\main\\resources\\sensor.txt")

        val dataStream = inputStream
            .map(data => {
                val dataArray = data.split(",")
                SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
            })
//            .assignAscendingTimestamps(_.timestamp * 1000L)    //指定生成窗口的时间戳
        //定义watermark
            /*.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
                    override def extractTimestamp(t: SensorReading): Long = {
                        t.timestamp * 1000L
                    }
                }
            )*/
            .assignTimestampsAndWatermarks( new MyWMAssigner(1000L))

        val resultStream = dataStream
            .keyBy("id")
            //                .window( EventTimeSessionWindows.withGap(Time.minutes(1)) )  //会话窗口
            //                .window( TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)) )
            .timeWindow(Time.seconds(15), Time.seconds(5))
            .allowedLateness(Time.minutes(1))    //输出一次计算结果，等一分钟在关闭
            .sideOutputLateData( new OutputTag[SensorReading]("late") )  //侧输出流，存放未及时处理的乱序数据
//            .reduce( new MyReduce )
            .apply( new MyWindowFun )

        //输出打印
        resultStream.print("result")
        //获取侧输出流数据
        resultStream.getSideOutput( new OutputTag[SensorReading]("late") )
        env.execute("window test")
    }
}

//定义一个全窗口函数
class MyWindowFun() extends WindowFunction[SensorReading, (String, Long, Int), Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[SensorReading], out: Collector[(String, Long, Int)]): Unit = {
        // 提前key
        val id = key.asInstanceOf[Tuple1[String]].f0
        out.collect((id, window.getStart, input.size))
    }
}

// 自定义一个周期性生成watermark的Assigner
class MyWMAssigner(lateness: Long) extends AssignerWithPeriodicWatermarks[SensorReading] {
    // 需要两个关键参数，延迟时间，和当前所有数据中最大的时间戳
    var maxTs: Long = Long.MinValue + lateness

    override def getCurrentWatermark: Watermark = {
        new Watermark(maxTs - lateness)
    }

    override def extractTimestamp(t: SensorReading, l: Long): Long = {
        maxTs = maxTs.max(t.timestamp * 1000L)
        t.timestamp * 1000L
    }
}

// 自定义一个断点式生成watermark的Assigner
class MyWMAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading]{
    val lateness: Long = 1000L
    override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
        if( t.id == "sensor_1" ) {
            new Watermark(l - lateness)
        } else null
    }

    override def extractTimestamp(t: SensorReading, l: Long): Long = {
        t.timestamp * 1000L
    }
}