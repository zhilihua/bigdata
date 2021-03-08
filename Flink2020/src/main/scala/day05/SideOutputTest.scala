package day05

import com.example.apitest.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2021/3/4 22:07
 */
object SideOutputTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.socketTextStream("hadoop102", 7777)
        val dataStream: DataStream[SensorReading] = inputStream
            .map( data => {
                val dataArray = data.split(",")
                SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble )
            } )

        // 用 ProcessFunction的侧输出流实现分流操作
        val highTempStream = dataStream
            .process(new SplitTempProcessor(30))

        val lowTempStream = highTempStream.getSideOutput(new OutputTag[(String, Double, Long)]("low-temp") )

        // 打印输出
        highTempStream.print("high")
        lowTempStream.print("low")

        env.execute("side output job")
    }
}

// 自定义 ProcessFunction，用于区分高低温度的数据
class SplitTempProcessor(threshold: Double) extends ProcessFunction[SensorReading, SensorReading]{
    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
        // 判断当前数据的温度值，如果大于阈值，输出到主流；如果小于阈值，输出到侧输出流
        if(i.temperature > threshold){
            collector.collect(i)
        }else {
            context.output( new OutputTag[(String, Double, Long)]("low-temp"),
                (i.id, i.temperature, i.timestamp))
        }
    }
}