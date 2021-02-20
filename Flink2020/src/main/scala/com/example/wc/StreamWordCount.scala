package com.example.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
      //创建流处理执行环境
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.disableOperatorChaining()    //全局拒绝slot合并
      //从程序运行参数读取host和port
//      val params = ParameterTool.fromArgs(args)
//      val hostname = params.get("host")
//      val port = params.getInt("port")
      //接收socket文本流
       val inputDataStream = env.socketTextStream("hadoop102", 7777)

      //定义转义操作
      val resultDataStream = inputDataStream
          .flatMap(_.split(" "))
          .filter(_.nonEmpty).slotSharingGroup("2")    //设置slot共享组，可以达到拒绝合并的效果
          .map((_, 1)).disableChaining()    //拒绝合并slot
          .keyBy(0)
          .sum(1).startNewChain()   //跟前面slot断开，也可以达到拒绝合并的效果

      resultDataStream.print()

      env.execute("stream word count job")
  }
}
