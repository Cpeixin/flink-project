package com.brent.flink

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time



object wordcount_streaming {
  def main(args: Array[String]): Unit = {
    // 定义一个数据类型保存单词出现的次数
    case class WordWithCount(word: String, count: Long)

    // port 表示需要连接的端口
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("9000")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 连接此socket获取输入数据
    val text = env.socketTextStream("localhost",port,'\n')

    //需要加上这一行隐式转换 否则在调用flatmap方法的时候会报错
    import org.apache.flink.api.scala
    import org.apache.flink.streaming.api.scala._
//    Error:(31, 15) could not find implicit value for evidence parameter of type org.apache.flink.api.common.typeinfo.TypeInformation[String]
    val result = text
      .flatMap(w => w.split(" "))
      .map(WordWithCount(_,1))
      .keyBy(_.word)
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // 打印输出并设置使用一个并行度
    result.print().setParallelism(1)
    env.execute("Socket Window WordCount")
  }
}
