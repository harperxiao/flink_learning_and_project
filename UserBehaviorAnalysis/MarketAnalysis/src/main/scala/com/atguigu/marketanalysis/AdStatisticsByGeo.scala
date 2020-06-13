package com.atguigu.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 输入的广告点击事件样例类
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long )
// 按照省份统计的输出结果样例类
case class CountByProvince( windowEnd: String, province: String, count: Long )
// 输出的黑名单报警信息
case class BlackListWarning( userId: Long, adId: Long, msg: String )

object AdStatisticsByGeo {
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blackList")
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 读取数据并转换成AdClickEvent
    val resource = getClass.getResource("/AdClickLog.csv")
    val adEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        AdClickEvent( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 自定义process function，过滤大量刷点击的行为
    val filterBlackStream = adEventStream
      .keyBy(data => (data.userId,data.adId))
      .process(new FilterBlackListUser(100))

    // 根据省份做分组，开窗聚合
    val adCountStream = filterBlackStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1),Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdcountResult())


    adCountStream.print("count")
    filterBlackStream.getSideOutput(blackListOutputTag).print("blackList")

    env.execute("ad statistics job")

  }

  class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long,Long),AdClickEvent,AdClickEvent]{
    // 定义状态，保存当前用户对当前广告的点击量
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state", classOf[Long]))
    // 保存是否发送过黑名单的状态
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState( new ValueStateDescriptor[Boolean]("issent-state", classOf[Boolean]) )
    // 保存定时器触发的时间戳
    lazy val resetTimer: ValueState[Long] = getRuntimeContext.getState( new ValueStateDescriptor[Long]("resettime-state", classOf[Long]) )

    override def processElement(i: AdClickEvent, context: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, collector: Collector[AdClickEvent]): Unit = {

      val curCount = countState.value()

      // 如果是第一次处理，注册定时器，每天00：00触发
      if(curCount == 0) {
        val ts = ( context.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (1000*60*60*24)
        resetTimer.update(ts)
        context.timerService().registerProcessingTimeTimer(ts)
      }

      if (curCount >= maxCount){
        if (!isSentBlackList.value()){
          isSentBlackList.update(true)
          context.output(blackListOutputTag,BlackListWarning(i.userId,i.adId, "Click over " + maxCount + " times today."))
        }
        return
      }

      countState.update(curCount + 1)
      collector.collect(i)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      if (timestamp == resetTimer.value()){
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }

}

class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{
  override def add(in: AdClickEvent, acc: Long): Long = acc + 1

  override def createAccumulator(): Long = 0L

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class AdcountResult() extends WindowFunction[Long,CountByProvince,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    out.collect(CountByProvince(new Timestamp(window.getEnd).toString,key,input.iterator.next()))
  }
}
