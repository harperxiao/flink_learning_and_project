package com.hp.apitest

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream: DataStream[String] = env.socketTextStream("localhost", 8888)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val highTempStream: DataStream[SensorReading] = dataStream
      .process(new SplitTempMonitor())

    highTempStream.print("high")
    highTempStream.getSideOutput(new OutputTag[(String,Double,Long)]("lowtemp")).print("low")

    env.execute("sideouput test")
  }
}

class SplitTempMonitor() extends ProcessFunction[SensorReading,SensorReading]{
  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temp>=30)
      collector.collect(i)
    else
      context.output(new OutputTag[(String,Double,Long)]("lowtemp"),(i.id,i.temp,i.timestamp))
  }
}
