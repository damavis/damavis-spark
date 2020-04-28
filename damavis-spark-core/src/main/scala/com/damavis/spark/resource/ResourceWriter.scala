package com.damavis.spark.resource

import org.apache.spark.sql.DataFrame

trait ResourceWriter {
  def write(data: DataFrame): Unit
}
