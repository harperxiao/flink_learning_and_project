package com.hp.apitest

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichReduceFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.util.Collector



object StateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)

//    env.setStateBackend(new MemoryStateBackend())
//    env.setStateBackend(new FsStateBackend(""))
//    env.setStateBackend(new RocksDBStateBackend(""))
//    env.enableCheckpointing(1000L)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
//    env.getCheckpointConfig.setCheckpointTimeout(60000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)
//    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)
//    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)
//
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000L))
//    env.setRestartStrategy(RestartStrategies.failureRateRestart(5,Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))


    val inputStream: DataStream[String] = env.socketTextStream("localhost", 8888)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultStream: DataStream[(String,Double,Double)] = dataStream
      .keyBy(_.id)
//      .reduce(new MyStateTestFunc())
      .flatMap(new TempChangeAlert(10.0))

    val resultStream2: DataStream[(String, Double, Double)] = dataStream
      .keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double]({
      case (input: SensorReading,None) => (List.empty,Some(input.temp))
      case (input: SensorReading,lastTemo: Some[Double]) => {
        val diff = (input.temp - lastTemo.get).abs
        if (diff>10.0){
          ( List( (input.id, lastTemo.get, input.temp) ), Some(input.temp) )
        }else{
          (List.empty,Some(input.temp))
        }
      }
      }
    )

    dataStream.print("data")
//    resultStream.print("result")
    resultStream2.print("result2")
    env.execute("state test")
  }
}

class MyStateTestFunc() extends RichReduceFunction[SensorReading]{
  lazy val myValueState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("myvalue",classOf[Double]))
  lazy val myListState: ListState[String] = getRuntimeContext.getListState(new ListStateDescriptor[String]("mylist",classOf[String]))
  lazy val myMaoState: MapState[String,Int] = getRuntimeContext.getMapState(new MapStateDescriptor[String,Int]("MAMAP",classOf[String],classOf[Int]))
  lazy val myReducingState: ReducingState[SensorReading] = getRuntimeContext.getReducingState( new ReducingStateDescriptor[SensorReading]("myReduce", new MyReduceFunc, classOf[SensorReading]) )

  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    val myValue:Double = myValueState.value()
    import scala.collection.JavaConversions._
    val myList: Iterable[String] = myListState.get()
    myMaoState.get("sensor_1")
    myReducingState.get()

    myValueState.update( 0.0 )
    myListState.add("hello flink")
    myMaoState.put("sensor_1", 1)
    myReducingState.add(t1)

    t1
  }
}

class TempChangeAlert(thredshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{
  // 定义一个状态，用来保存上一次的温度值
  //  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState( new ValueStateDescriptor[Double]("last-temp", classOf[Double]) )
  private var lastTempState: ValueState[Double] = _
  private var isFirstTempState: ValueState[Boolean] = _
  override def open(parameters: Configuration): Unit = {
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    isFirstTempState = getRuntimeContext.getState( new ValueStateDescriptor[Boolean]("is-firsttemp", classOf[Boolean], true) )
  }

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    // 获取状态，拿到上次的温度值
    val lastTemp = lastTempState.value()

    lastTempState.update(in.temp)

    // 对比两次温度值，如果大于阈值，报警
    val diff = (in.temp - lastTemp).abs
    if (diff>thredshold && !isFirstTempState.value()){
      collector.collect((in.id,lastTemp,in.temp))
    }
    isFirstTempState.update(false)

  }

}
