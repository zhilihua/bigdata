package com.example.wc

import org.apache.flink.api.scala._

/**
 * 批处理
 */
object WordCount {
    def main(args: Array[String]): Unit = {
        //处理一个批处理的执行环境
        val env = ExecutionEnvironment.getExecutionEnvironment
        //从文件中读取数据
        val inputDataSet = env.readTextFile("E:\\Idea\\bigdata\\Flink2020\\src\\main\\resources\\word.txt")
        //基于DataSet做转换，首先按空格分词打散，然后按照word作为key做group by
        val resultDataSet = inputDataSet.flatMap(_.split(" "))
            .map((_, 1))
            .groupBy(0)
            .sum(1)
        //打印输出

        resultDataSet.print()
    }
}
