package com.lolo.flink.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object TestUnion {

  def main(args: Array[String]): Unit = {
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.streaming.api.scala._

    val stream1 = streamEnv.fromElements(("a", 1), ("b", 2))
    val stream2 = streamEnv.fromElements(("b", 5), ("d", 6))
    val stream3 = streamEnv.fromElements(("e", 7), ("f", 8))

    val result: DataStream[(String, Int)] = stream1.union(stream2, stream3)

    result.print()

    streamEnv.execute()


  }
}
