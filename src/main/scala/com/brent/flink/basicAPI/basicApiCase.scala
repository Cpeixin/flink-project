package com.brent.flink.basicAPI

import com.google.gson.{JsonObject, JsonParser}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * 创建环境
  * 读取数据
  * 处理数据
  * 数据输出
  * 默认多线程输出，setParallelism(1)指定并行数
  */


object basicApiCase {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.readTextFile("data/basicApiCaseData")
    import org.apache.flink.streaming.api.scala._

    val result = text.map(x=>{
      val json = new JsonParser()
      val obj = json.parse(x).asInstanceOf[JsonObject]
      obj.get("P1-PH") //Gson使用
    })

    result.print()
    result.print().setParallelism(1)//用一个线程打印结果

    result.writeAsText("data/basicApiCaseResult/Multithreading")
    result.writeAsText("data/basicApiCaseResult/singlethreading").setParallelism(1)//用一个线程打印结果

    env.execute("basicApiCase") //执行算子
  }
}
