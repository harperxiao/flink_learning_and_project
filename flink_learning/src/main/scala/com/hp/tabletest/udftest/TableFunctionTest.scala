package com.hp.tabletest.udftest

import com.hp.apitest.SensorReading
import org.apache.commons.math3.geometry.spherical.oned.ArcsSet.Split
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt")
    //    val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      } )
    // 将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temp, 'timestamp.rowtime as 'ts)

    val split = new Split("_")

    val resultTable = sensorTable
        .joinLateral(split('id) as ('word,'length))
        .select('id,'ts,'word,'length)

    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)

    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,ts,word,length
        |from
        |sensor,lateral table(split(id)) as splitid(word,length)
      """.stripMargin)

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("table funtion test")
  }
}

class Split(separator: String) extends TableFunction[(String,Int)]{
  def eval(str:String): Unit={
    str.split(separator).foreach(
      word => collect((word,word.length))
    )
  }
}
