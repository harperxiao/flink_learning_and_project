package com.hp.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random


case class SensorReading(id: String, timestamp: Long, temp: Double)
object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)

    val stream1: DataStream[SensorReading] = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ))
    //    val stream = env.fromElements(1, 0.435, "hello", ("word", 1))

    val stream2: DataStream[String] = env.readTextFile("F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt")

    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor",new SimpleStringSchema(),properties))

    val stream4: DataStream[SensorReading] = env.addSource(new MySensorSource())
    //    stream1.print("stream1")
//    stream2.print("stream2")
//    stream3.print("stream3")
    stream4.print("stream4")
    env.execute("source test job")


  }



}

class MySensorSource() extends RichSourceFunction[SensorReading]{

  var running: Boolean = true
  override def cancel(): Unit = {
    running = false
  }

  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {

    val rand = new Random()

    var curtemp = 1.to(10).map(
      i => ("sensor_"+i, 60 + rand.nextGaussian()*20)
    )

    while(running){

      curtemp = curtemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )

      var curTs = System.currentTimeMillis()
      curtemp.foreach(
        data => ctx.collect(SensorReading(data._1,curTs,data._2))
      )

      Thread.sleep(200)
    }
  }
}
