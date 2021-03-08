package day04

import com.example.apitest.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2021/3/4 12:36
 */
object ProcessFunctionTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.socketTextStream("hadoop102", 7777)
        val dataStream = inputStream
            .map(data => {
                val dataArray = data.split(",")
                SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
            })

        // 检测每一个传感器温度是否连续上升，在10秒之内
        val warningStream = dataStream
            .keyBy("id")
            .process( new TempIncreWarning(10000L) )

        warningStream.print()
        env.execute("process function job")
    }
}

// 自定义 KeyedProcessFunction
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {
    // 由于需要跟之前的温度值做对比，所以将上一个温度保存成状态
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
    // 为了方便删除定时器，还需要保存定时器的时间戳
    lazy val curTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-timer-ts", classOf[Long]))

    override def processElement(i: SensorReading, context: KeyedProcessFunction[Tuple, SensorReading, String]#Context, collector: Collector[String]): Unit = {
        //首先取出状态
        val lastTemp = lastTempState.value()
        val curTimerTs = curTimerTsState.value()

        //将上次温度值的状态更新为当前数值
        lastTempState.update(i.temperature)

        //判断当前温度值，如果比之前温度高，并且没有定时器的话，注册10秒后的定时器
        if(i.temperature > lastTemp && curTimerTs == 0){
            val ts = context.timerService().currentProcessingTime() + interval
            context.timerService().registerProcessingTimeTimer(ts)
            curTimerTsState.update(ts)
        }
        // 如果温度下降，删除定时器
        else if( i.temperature < lastTemp ) {
            //删除定时器
            context.timerService().deleteProcessingTimeTimer(curTimerTs)
            //清空状态
            curTimerTsState.clear()
        }
    }

    // 定时器触发，说明10秒内没有来下降的温度值，报警
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect("温度值连续" + interval / 1000 + "秒上升")
        curTimerTsState.clear()
    }
}