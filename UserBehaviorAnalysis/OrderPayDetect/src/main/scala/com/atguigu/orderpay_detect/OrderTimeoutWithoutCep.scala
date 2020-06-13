package com.atguigu.orderpay_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCep {
  val orderTimeOutputTag = new OutputTag[OrderResult]("orderTimeout")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取订单数据
    val resource = getClass.getResource("/OrderLog.csv")
    //    val orderEventStream = env.readTextFile(resource.getPath)
    val orderEventStream = env.socketTextStream("localhost", 8888)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.orderId)

    // 定义process function进行超时检测
    val orderResultStream = orderEventStream.process(new OrderPayMatch())

    orderResultStream.print("payed")
    orderResultStream.getSideOutput(orderTimeOutputTag).print("timeout")

    env.execute("order timeout without cep job")
  }

  class OrderPayMatch() extends KeyedProcessFunction[Long, OrderEvent, OrderResult]{
    lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))
    // 保存定时器的时间戳为状态
    lazy val timerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state", classOf[Long]))

    override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, collector: Collector[OrderResult]): Unit = {
      // 先读取状态
      val isPayed = isPayedState.value()
      val timerTs = timerState.value()

      if (i.eventType =="create"){
        // 1. 如果是create事件，接下来判断pay是否来过
        if(isPayed){
          // 1.1 如果已经pay过，匹配成功，输出主流，清空状态
          collector.collect(OrderResult(i.orderId,"payed"))
          context.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        }else{
          // 1.2 如果没有pay过，注册定时器等待pay的到来
          val ts = i.eventTime * 1000L + 15 * 60 * 1000L
          context.timerService().registerEventTimeTimer(ts)
          timerState.update(ts)
        }
      }else if(i.eventType == "pay"){
        // 2. 如果是pay事件，那么判断是否create过，用timer表示
        if (timerTs>0){
          // 2.1 如果有定时器，说明已经有create来过
          // 继续判断，是否超过了timeout时间
          if(timerTs > i.eventTime * 1000L){
            // 2.1.1 如果定时器时间还没到，那么输出成功匹配
            collector.collect( OrderResult(i.orderId, "payed successfully") )
          }else{
            // 2.1.2 如果当前pay的时间已经超时，那么输出到侧输出流
            context.output(orderTimeOutputTag, OrderResult(i.orderId, "payed but already timeout"))
          }
          context.timerService().deleteEventTimeTimer(timerTs)
          isPayedState.clear()
          timerState.clear()
        }else{
          // 2.2 pay先到了，更新状态，注册定时器等待create
          isPayedState.update(true)
          context.timerService().registerEventTimeTimer(i.eventTime*1000L)
          timerState.update(i.eventTime*1000L)
        }
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
      if (isPayedState.value()){
        // 如果为true，表示pay先到了，没等到create
        ctx.output(orderTimeOutputTag, OrderResult(ctx.getCurrentKey, "already payed but not found create log"))
      }else{
        // 表示create到了，没等到pay
        ctx.output(orderTimeOutputTag, OrderResult(ctx.getCurrentKey, "order timeout"))
      }
      isPayedState.clear()
      timerState.clear()
    }
  }

}
