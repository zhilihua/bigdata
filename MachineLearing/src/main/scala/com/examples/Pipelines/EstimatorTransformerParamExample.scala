package com.examples.Pipelines

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{Row, SparkSession}

object EstimatorTransformerParamExample {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("EstimatorTransformerParamExample")
            .getOrCreate()

        //准备训练数据从一个列表
        val training = spark.createDataFrame(Seq(
            (1.0, Vectors.dense(0.0, 1.1, 0.1)),
            (0.0, Vectors.dense(2.0, 1.0, -1.0)),
            (0.0, Vectors.dense(2.0, 1.3, 1.0)),
            (1.0, Vectors.dense(0.0, 1.2, -0.5))
        )).toDF("label", "features")

        //创建一个逻辑回归实例
        val lr = new LogisticRegression()
        //打印默认参数
        println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")

        //重置模型参数
        lr.setMaxIter(10).setRegParam(0.01)

        //训练模型1
        val model1 = lr.fit(training)
        //打印参数
        println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)
        //使用参数指定模式传入参数
        val paramMap = ParamMap(lr.maxIter -> 20)
            .put(lr.maxIter, 30)
            .put(lr.regParam -> 0.1, lr.threshold -> 0.55)

        val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")

        val paraMapCombined = paramMap ++ paramMap2
        //训练模型2
        val model2= lr.fit(training, paraMapCombined)
        //打印模型2参数
        println("Model 2 was fit using parameters: " + model2.parent.extractParamMap)

        //准备测试数据
        val test = spark.createDataFrame(Seq(
            (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
            (0.0, Vectors.dense(3.0, 2.0, -0.1)),
            (1.0, Vectors.dense(0.0, 2.2, -1.5))
        )).toDF("label", "features")

        //测试验证
        model2.transform(test)
            .select("features", "label", "myProbability", "prediction")
            .collect()
            .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
                println(s"($features, $label) -> prob=$prob, prediction=$prediction")
            }

        spark.stop()
    }
}
