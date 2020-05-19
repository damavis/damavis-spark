package com.damavis.spark.pipeline3

import org.apache.spark.sql.DataFrame

trait Processor {

  private var _in: DataFrame = null

  protected def in: DataFrame = _in

  def setIn(v: DataFrame): Unit = _in = v

  def compute(): DataFrame
}
