package com.hp.tabletest

import com.hp.apitest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Over, Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TimeAndWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val TableEnv = StreamTableEnvironment.create(env)

//    val inputStream: DataStream[String] = env.readTextFile("F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt")
    val inputStream: DataStream[String] = env.socketTextStream("localhost", 8888)
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000L
      })

    // 将流转换成表，直接定义时间字段
    val sensorTable: Table = TableEnv.fromDataStream(dataStream,'id,'temp,'timestamp.rowtime as 'ts)

    // 1. Table API
    // 1.1 Group Window聚合操作
    val resultTable: Table = sensorTable
      .window(Tumble over 10.seconds on 'ts as 'tw)
      .groupBy('id,'tw)
      .select('id,'id.count,'tw.end)

    // 1.2 Over Window 聚合操作
    val overResltTable:Table = sensorTable
      .window(Over partitionBy 'id orderBy 'ts preceding 2.rows as 'ow)
      .select('id,'ts,'id.count over 'ow,'temp.avg over 'ow)

    // 2. SQL实现
    // 2.1 Group Windows
    TableEnv.createTemporaryView("sensor",sensorTable)
    val resultSqlTable: Table = TableEnv.sqlQuery(
      """
        |select id,count(id),hop_end(ts,interval '4' second,interval '10' second)
        |from sensor
        |group by id,hop(ts, interval '4' second, interval '10' second)
      """.stripMargin
    )

    // 2.2 Over Window
    val orderSqlTable: Table = TableEnv.sqlQuery(
      """
        |select id,ts,count(id) over w,avg(temp) over w
        |from sensor
        |window w as(
        | partition by id
        | order by ts
        | rows between 2 preceding and current row
        |)
      """.stripMargin)

    //    sensorTable.printSchema()
    // 打印输出
    dataStream.print("data")
//    resultTable.toRetractStream[Row].print("agg")
//    overResltTable.toAppendStream[Row].print("over result")
//    resultSqlTable.toRetractStream[Row].print("agg sql")
    orderSqlTable.toAppendStream[Row].print("ordersql")

    env.execute("time and window test ")



  }
}
