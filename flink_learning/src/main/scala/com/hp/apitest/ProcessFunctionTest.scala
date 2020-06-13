package com.hp.apitest

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ProcessFunctionTest {
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

    val resultStream = dataStream
      .keyBy(_.id)
//      .process(new MyKeyedProcessFunction())
      .process(new TempIncreWarning(10000L))

    resultStream.print()
    env.execute("process test job")
  }
}

class MyKeyedProcessFunction() extends KeyedProcessFunction[String,SensorReading,String]{
  override def processElement(i: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    ctx.output(new OutputTag[String]("side"),i.id)
    ctx.timerService().currentWatermark()
    ctx.timerService().registerEventTimeTimer(i.timestamp*1000L+1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

  }
}

// 自定义Process Function，检测10秒之内温度连续上升
class TempIncreWarning(interval: Long) extends KeyedProcessFunction[String,SensorReading,String]{
  // 定义一个ValueState，用来保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
  // 定义一个状态，用来保存设置的定时器时间戳
  lazy val curTimerState: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("cur-timer", classOf[Long]) )


  override def processElement(i: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    // 取出上一次温度值
    val lastTemp: Double = lastTempState.value()
    val curTimer: Long = curTimerState.value()

    // 更新温度值状态
    lastTempState.update(i.temp)

    // 将当前的温度值，跟上次的比较
    if (i.temp>lastTemp && curTimer ==0){
      // 如果温度上升，且没有注册过定时器，那么按当前时间加10s注册定时器
      val ts = ctx.timerService().currentProcessingTime()+interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      curTimerState.update(ts)
    }else if(i.temp<lastTemp){
      // 如果温度下降，那么直接删除定时器，重新开始
      ctx.timerService().deleteProcessingTimeTimer(curTimer)
      curTimerState.clear()
    }

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("传感器 " + ctx.getCurrentKey + " 的温度值连续" + interval / 1000 + "秒上升" )
    curTimerState.clear()
  }
}
