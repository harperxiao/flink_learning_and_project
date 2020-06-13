package com.hp.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val parameterTool: ParameterTool = ParameterTool.fromArgs(args)
    val hostname: String = parameterTool.get("host")
    val port: Int = parameterTool.getInt("port")

    val inputDataStream: DataStream[String] = environment.socketTextStream(hostname,port)

    val result: DataStream[(String, Int)] = inputDataStream
      .flatMap(_.split(" ")).startNewChain()
      .map((_, 1))
      .keyBy(_._1)
      .sum(1)

    result.print()

    environment.execute("stream wc job")
  }
}
