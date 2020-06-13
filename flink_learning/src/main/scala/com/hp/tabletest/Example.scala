package com.hp.tabletest

import com.hp.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object Example {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)


    val inputStream: DataStream[String] = env.readTextFile("F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt")
//    val inputStream: DataStream[String] = env.socketTextStream("localhost", 8888)

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val tableEnv = StreamTableEnvironment.create(env)

    var dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 3. 转换操作，得到提取结果
    // 3.1 调用table api，做转换操作
    val resultTable: Table = dataTable
      .select("id,temp")
      .filter("id == 'sensor_!'")
    // 3.2 写sql实现转换
    //    tableEnv.registerTable("dataTable", dataTable)
    tableEnv.createTemporaryView("dataTable",dataTable)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id,temp
        |from dataTable
        |where id='sensor_1'
      """.stripMargin)

    // 4. 把表转换成流，打印输出
    val resultStream: DataStream[(String,Double)] = resultSqlTable
      .toAppendStream[(String,Double)]

    resultStream.print()
    env.execute("table api test")




  }
}
