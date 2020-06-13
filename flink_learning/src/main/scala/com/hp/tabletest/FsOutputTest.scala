package com.hp.tabletest

import com.hp.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object FsOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val filePath: String = "F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(filePath)
    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 3. 把流转换成表
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temp, 'timestamp as 'ts)
    val resultTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    tableEnv.connect(new FileSystem().path("F:\\learning\\code\\flink_learning\\src\\main\\resources\\output2"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("output")

    resultTable.insertInto("output")

    env.execute("fs output")
  }
}
