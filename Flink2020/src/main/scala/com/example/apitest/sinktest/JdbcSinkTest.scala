package com.example.apitest.sinktest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.example.apitest.{MySensorSource, SensorReading}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JdbcSinkTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //读取数据
        val dataStream = env.addSource(new MySensorSource)

        //落地数据


        //启动程序
        env.execute("jdbc sink test")
    }
}

// 自定义一个SinkFunction
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
    //首先定义sql连接，以及预编译语句
    var conn: Connection = _
    var insertStmt: PreparedStatement = _
    var updateStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456")
        insertStmt = conn.prepareStatement("insert into temp (sensor, temperature) values (?,?)")
        updateStmt = conn.prepareStatement("update temp set temperature = ? where sensor = ?")
    }

    override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
        // 执行更新语句
        updateStmt.setDouble(1, value.temperature)
        updateStmt.setString(2, value.id)
        updateStmt.execute()
        // 如果刚才没有更新数据，那么执行插入语句
        if (updateStmt.getUpdateCount == 0) {
            insertStmt.setString(1, value.id)
            insertStmt.setDouble(2, value.temperature)
            insertStmt.execute()
        }
    }

    // 关闭操作
    override def close(): Unit = {
        insertStmt.close()
        updateStmt.close()
        conn.close()
    }
}