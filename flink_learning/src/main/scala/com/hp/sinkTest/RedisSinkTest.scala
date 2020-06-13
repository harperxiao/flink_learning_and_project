package com.hp.sinkTest

import com.hp.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

    val inputStream: DataStream[String] = env.readTextFile("F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt")

    val dataStream: DataStream[SensorReading] = inputStream
      .map( data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0),dataArray(1).toLong,dataArray(2).toDouble)
      })

    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf,new MyRedisMapper()))

    env.execute("redis sink job")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET,"sensor_tmp")

  override def getValueFromData(t: SensorReading): String = t.temp.toString

  override def getKeyFromData(t: SensorReading): String = t.id
}
