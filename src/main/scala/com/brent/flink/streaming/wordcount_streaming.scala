package com.brent.flink.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * DataStream API wordcount示例
  */

object wordcount_streaming {
  def main(args: Array[String]): Unit = {
    // 定义一个数据类型保存单词出现的次数
    case class WordWithCount(word: String, count: Long)

    // port 表示需要连接的端口
    val port: Int = try {
      //      ParameterTool.fromArgs(args).getInt("port")
      9000
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")

        return
      }
    }
    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 连接此socket获取输入数据
    val text = env.socketTextStream("localhost", port, '\n')

    //需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错
    import org.apache.flink.streaming.api.scala._
    //    Error:(31, 15) could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[String]
    val result = text
      .flatMap(w => w.split(" "))
      .map(WordWithCount(_, 1))
      .keyBy(_.word)
      //        .countWindow(3)
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // 打印输出并设置使用一个并行度
    result.print().setParallelism(1)
    env.execute("Socket Window WordCount")
  }
}

//keyby(_key)
/*
逻辑上将流分区为不相交的分区。具有相同密钥的所有记录都分配给同一分区。在内部，keyBy（）是使用散列分区实现的。指定键有多种方法。
eg: keyby("column") or keyby(0)

并发度：
分组数据流将你的window计算通过多任务并发执行，以为每一个逻辑分组流在执行中与其他的逻辑分组流是独立地进行的。
在非分组数据流中，你的原始数据流并不会拆分成多个逻辑流并且所有的window逻辑将在一个任务中执行，并发度为1。
 */
