package com.damavis.spark.pipeline3
import org.apache.spark.sql.DataFrame

abstract class LinealProcessor extends Processor {

  def computeImpl(data: DataFrame): DataFrame

  override def compute(sockets: SocketSet): DataFrame = {
    computeImpl(sockets.left.get)
  }
}
