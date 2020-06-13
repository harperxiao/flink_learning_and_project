package com.hp.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val inputStream: DataStream[String] = env.readTextFile("F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map( data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

    val aggStream: DataStream[SensorReading] = dataStream.keyBy(_.id).minBy("temp")

    val reduceStream: DataStream[SensorReading] = dataStream
      .keyBy(_.id)
      .reduce( (curState, newData) => {
        SensorReading(curState.id,newData.timestamp+1,curState.temp.min(newData.temp))
      } )

    val splitStream: SplitStream[SensorReading] = dataStream
      .split( data => {
        if(data.temp>30)
          Seq("high")
        else
          Seq("low")
      })

    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high","low")

    val highWaringStream: DataStream[(String,Double)] = highTempStream
      .map(data =>(data.id,data.temp))

    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = highWaringStream
      .connect(lowTempStream)

    val coMapStream: DataStream[(String, Double, String)] = connectedStreams
        .map(
          highWaringData =>(highWaringData._1,highWaringData._2,"warning"),
          lowTempData => (lowTempData.id, lowTempData.temp, "normal")
        )

    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream)

    val resultStream: DataStream[SensorReading] = dataStream
      .filter( new MyFilter() )

    // 打印输出
//    dataStream.print("data")
//    aggStream.print("agg")
//        reduceStream.print("reduce")
//        highTempStream.print("high")
    //    lowTempStream.print("low")
    //    allTempStream.print("all")
//        coMapStream.print("coMap")

    resultStream.print("result")

    env.execute("transform test job")
  }
}

class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

class MyRichMapper extends RichMapFunction[SensorReading,String]{

  override def close(): Unit = super.close()

  override def open(parameters: Configuration): Unit = super.open(parameters)

  override def map(in: SensorReading): String = in.id
}
