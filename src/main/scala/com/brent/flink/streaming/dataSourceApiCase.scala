package com.brent.flink.streaming

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.typeinfo.BasicTypeInfo

object dataSourceApiCase {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val path = "data/dataSource/data"
    val format = new TextInputFormat(new Path(path))

    val typeInfo = BasicTypeInfo.STRING_TYPE_INFO


    val original = env.readFile(format, "data/dataSource/data")
    val original_1 = env.readTextFile("data/dataSource/data")

//    val original_2 = env.readFile(format, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 1L)
    val original_2 = env.readFile(format, path, FileProcessingMode.PROCESS_ONCE, 1L)


//    original.print()
//    original_1.print()
    original_2.print()

    env.execute()

  }
}


/*
readFile(format,path)函数
format的格式指定如上
其中，还有更多格式在org.apache.flink.api.java.io.*中
eg: CsvInputFormat  RowCsvInputFormat  TypeSerializerInputFormat
 */



/*
original_2  指定 FileProcessingMode.PROCESS_CONTINUOUSLY 参数
可以定期监视（每intervalms）新数据
在修改文件时，将完全重新处理其内容。
这可以打破“完全一次”的语义，因为在文件末尾附加数据将导致其 ***所有内容被重新处理。****
*每次更新文件内容，文件中所有的数据全部重新计算*

参数：FileProcessingMode.PROCESS_ONCE
只处理一次文件中的所有数据

 */