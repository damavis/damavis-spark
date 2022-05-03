package com.damavis.spark.dataflow

import org.apache.spark.sql.DataFrame

abstract class SourceProcessor extends Processor {
  def computeImpl(): DataFrame

  override def compute(sockets: SocketSet): DataFrame = computeImpl()
}
