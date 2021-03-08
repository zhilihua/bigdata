package day05

import java.util
import java.util.concurrent.TimeUnit

import com.example.apitest.SensorReading
import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2021/3/5 10:38
 */
object StateTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        // 配置状态后端
//            env.setStateBackend( new MemoryStateBackend() )
        env.setStateBackend( new FsStateBackend("") )
//            env.setStateBackend( new RocksDBStateBackend("", true) )

        // checkpoint相关配置
        // 启用检查点，指定触发检查点的间隔时间(毫秒)
        env.enableCheckpointing(10000L)
        // 其它配置
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
        env.getCheckpointConfig.setCheckpointTimeout(30000L)
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
        env.getCheckpointConfig.setPreferCheckpointForRecovery(false)
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

        // 重启策略的配置
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))


        val inputStream = env.socketTextStream("hadoop102", 7777)
        val dataStream: DataStream[SensorReading] = inputStream
            .map( data => {
                val dataArray = data.split(",")
                SensorReading( dataArray(0), dataArray(1).toLong, dataArray(2).toDouble )
            } )

        val warningStream = dataStream
            .keyBy("id")
//            .flatMap(new TempChangeWarningWithFlatmap(10.0))
            .flatMapWithState( {
                case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
                case (inputData: SensorReading, lastTemp: Some[Double]) => {
                    val diff = (inputData.temperature - lastTemp.get).abs
                    if(diff > 10.0) {
                        (List(inputData.id, lastTemp.get, inputData.temperature), Some(inputData.temperature))
                    }else {
                        (List.empty, Some(inputData.temperature))
                    }
                }
            })
        warningStream.print()
        env.execute("state test job")
    }
}

// 自定义 RichMapFunction
class TempChangeWarning(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)]{
    // 定义状态变量，上一次的温度值
    private var lastTempState: ValueState[Double] = _

    override def open(parameters: Configuration): Unit = {
        lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    }

    override def map(in: SensorReading): (String, Double, Double) = {
        // 从状态中取出上次的温度值
        val lastTemp = lastTempState.value()
        // 更新状态
        lastTempState.update(in.temperature)

        // 跟当前温度值计算差值，然后跟阈值比较，如果大于就报警
        val diff = (in.temperature - lastTemp).abs
        if(diff > threshold) {
            (in.id, lastTemp, in.temperature)
        }else {
            (in.id, 0.0, 0.0)
        }
    }
}

// 自定义 RichFlatMapFunction，可以输出多个结果
class TempChangeWarningWithFlatmap(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
        // 从状态中取出上次的温度值
        val lastTemp = lastTempState.value()
        // 更新状态
        lastTempState.update(in.temperature)

        // 跟当前温度值计算差值，然后跟阈值比较，如果大于就报警
        val diff = (in.temperature - lastTemp).abs
        if(diff > threshold) {
            collector.collect((in.id, lastTemp, in.temperature))
        }
    }
}

// keyed state定义示例
class MyProcessor extends KeyedProcessFunction[String, SensorReading, Int]{
//    lazy val myState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))
    lazy val myListState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("my-liststate", classOf[String]))
    lazy val myMapState: MapState[String, Double] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("my-mapstate", classOf[String], classOf[Double]))
    getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading](
        "my-reducingstate",
        new MyReduceFunction,
        classOf[SensorReading]
    ))

    var myState: ValueState[Int] = _
    override def open(parameters: Configuration): Unit = {
        myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))
    }

    override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, Int]#Context, collector: Collector[Int]): Unit = {
        myState.value()
        myState.update(1)
        myListState.add("hello")
        myListState.update(new util.ArrayList[String]())
        myMapState.put("sensor_1", 10.0)
        myMapState.get("sensor_1")
    }
}

class MyReduceFunction() extends ReduceFunction[SensorReading] {
    override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
        SensorReading(t.id, t.timestamp.max(t1.timestamp), t.temperature.min(t1.temperature))
    }
}

// operator state示例
class MyMapper() extends RichMapFunction[SensorReading, Long] with ListCheckpointed[Long]{
    //定义全局状态
    var count = 0L
    override def map(in: SensorReading): Long = {
        count += 1
        count
    }

    override def snapshotState(l: Long, l1: Long): util.List[Long] = {
        val stateList = new util.ArrayList[Long]()
        stateList.add(count)
        stateList
    }

    override def restoreState(list: util.List[Long]): Unit = {
//        val iter = list.iterator()
//        while (iter.hasNext){
//            count += iter.next()
//        }
        import scala.collection.JavaConversions._
        for (countState <- list) {
            count += countState
        }
    }
}