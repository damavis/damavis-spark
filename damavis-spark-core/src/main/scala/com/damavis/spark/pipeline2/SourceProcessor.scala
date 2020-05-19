package com.damavis.spark.pipeline2
import org.apache.spark.sql.DataFrame

abstract class SourceProcessor extends Processor {
  def computeImpl(): DataFrame

  override def compute(sockets: SocketSet): DataFrame = computeImpl()
}
