package com.brent.flink.basicAPI

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object tupleCase {
  case class WordCount(word: String, count: Int)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input = env.fromElements(
      WordCount("hello", 1),
      WordCount("world", 2),WordCount("world",4)) // Case Class Data Set

    input.keyBy("word").print()// key by field expression "word"

    val input2 = env.fromElements(("hello", 1), ("world", 2),("world",4)) // Tuple2 Data Set

    input2.keyBy(0, 1).print() // key by field positions 0 and 1


    env.execute("tupleApi")
  }
}
