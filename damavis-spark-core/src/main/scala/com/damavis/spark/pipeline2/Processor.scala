package com.damavis.spark.pipeline2

import org.apache.spark.sql.DataFrame

trait Processor {
  def compute(sockets: SocketSet): DataFrame
}
