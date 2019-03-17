package com.brent.flink.streaming

import java.util.Properties
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

object dataSourceKafkaCase {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    group.id=test-consumer-group
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    //    properties.setProperty("zookeeper.connect", "localhost:2181") //only kafka 0.8 need this config
    properties.setProperty("group.id", "test-consumer-group")
    val stream = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))
      .print()


    env.execute()

  }
}
