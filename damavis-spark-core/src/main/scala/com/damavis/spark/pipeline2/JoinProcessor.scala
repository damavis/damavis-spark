package com.damavis.spark.pipeline2

import org.apache.spark.sql.DataFrame

abstract class JoinProcessor extends Processor {

  def computeImpl(left: DataFrame, right: DataFrame): DataFrame

  override def compute(sockets: SocketSet): DataFrame =
    computeImpl(sockets.left.get, sockets.right.get)
}
