package com.brent.flink

import org.apache.flink.api.scala.ExecutionEnvironment

object wordcount_batch {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text = env.readTextFile("data/wordcount")
    import org.apache.flink.api.scala._

    val counts = text.flatMap { _.toLowerCase.split(" ") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

//    counts.writeAsCsv(outputPath, "\n", " ")
    counts.print()
  }
}
