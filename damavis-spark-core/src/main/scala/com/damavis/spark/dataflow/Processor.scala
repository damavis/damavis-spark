package com.damavis.spark.dataflow

import org.apache.spark.sql.DataFrame

trait Processor {
  def compute(sockets: SocketSet): DataFrame
}
