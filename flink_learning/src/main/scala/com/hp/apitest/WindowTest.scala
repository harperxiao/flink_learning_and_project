package com.hp.apitest

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 8888)

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      //  .assignAscendingTimestamps( _.timestamp * 1000L )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(1000)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp*1000L
      })

    val resultStream: DataStream[SensorReading] = dataStream
      .keyBy(_.id)
    //     .window( SlidingProcessingTimeWindows.of(Time.hours(1), Time.seconds(10)) )
    //     .countWindow( 10, 2 )
    //     .window( EventTimeSessionWindows.withGap(Time.minutes(1)) )
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[SensorReading]("late"))
      .reduce(new MyReduceFunc())

    dataStream.print("data")
    resultStream.print("result")
    resultStream.getSideOutput(new OutputTag[SensorReading]("late")).print("late")

    env.execute("window test")
  }
}

class MyReduceFunc() extends ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id,t1.timestamp,t.temp.min(t1.temp))
  }
}

class MyAggFunc extends AggregateFunction[SensorReading,(Double,Int),Double]{
  override def add(in: SensorReading, acc: (Double, Int)): (Double, Int) =
    (acc._1 + in.temp,acc._2+1)

  override def createAccumulator(): (Double, Int) = (0.0,0)

  override def getResult(acc: (Double, Int)): Double = acc._1/acc._2

  override def merge(acc: (Double, Int), acc1: (Double, Int)): (Double, Int) =
    (acc._1+acc1._1,acc._2+acc1._2)
}

