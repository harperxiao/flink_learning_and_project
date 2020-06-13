package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

// 输入的登录事件样例类
case class LoginEvent( userId: Long, ip: String, eventType: String, eventTime: Long )
// 输出的异常报警信息样例类
case class Warning( userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginEvent {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取事件数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent( dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong )
      } )
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      } )

    val waringStrem = loginEventStream
      .keyBy(_.userId)
      .process(new LoginWaring(2))

    waringStrem.print()
    env.execute("login fail detect job")
  }
}

class LoginWaring(maxFailTimes: Int) extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  // 定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    if (i.eventType == "fail"){
      val iter = loginFailState.get().iterator()
      if (iter.hasNext){
        // 如果已经有登录失败事件，就比较事件时间
        val firstFail = iter.next()
        if(i.eventTime < firstFail.eventTime + 2){
          collector.collect(Warning(i.userId,firstFail.eventTime,i.eventTime,"login fail in 2 seconds." ))
        }
        loginFailState.clear()
        loginFailState.add(i)
      }else{
        // 如果是第一次登录失败，直接添加到状态
        loginFailState.add(i)
      }
    }else{
      // 如果是成功，清空状态
      loginFailState.clear()
    }
  }
}
