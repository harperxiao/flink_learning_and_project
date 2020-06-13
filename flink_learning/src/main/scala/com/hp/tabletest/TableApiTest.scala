package com.hp.tabletest

import javafx.scene.control.Tab

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object TableApiTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 1. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    /*
    // 1.1 老版本planner的流式查询
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useOldPlanner() // 用老版本
      .inStreamingMode() // 流处理模式
      .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settings)
    // 1.2 老版本批处理环境
    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val batchTableEnv: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)
    // 1.3 blink版本的流式查询
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val bsTableEnv = StreamTableEnvironment.create(env, bsSettings)
    // 1.4 blink版本的批式查询
    val bbSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val bbTableEnv = TableEnvironment.create(bbSettings)
    */

    val filePath = "F:\\learning\\code\\flink_learning\\src\\main\\resources\\sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new OldCsv()) // 定义从外部文件读取数据之后的格式化方法
      .withSchema(new Schema()
      .field("id", DataTypes.STRING())
      .field("timestamp", DataTypes.BIGINT())
      .field("temp", DataTypes.DOUBLE())
    ) // 定义表的结构
      .createTemporaryTable("inputTable") // 在表环境注册一张表

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    val sensorTable: Table = tableEnv.from("kafkaInputTable")

    val resultTable: Table = sensorTable
      .select('id,'temp)
      .filter('id === "sensor_1")

    val aggTable: Table = sensorTable
      .groupBy('id)
      .select('id,'id.count as 'count)

    val aggSqlTable: Table = tableEnv.sqlQuery("select id, count(id) as cnt from inputTable group by id")

    // 测试输出
    resultTable.toAppendStream[(String, Double)].print("result")
    aggTable.toRetractStream[(String, Long)].print("agg result")
    aggSqlTable.toRetractStream[(String, Long)].print("agg sql result")

    env.execute("table api test job")
  }
}
