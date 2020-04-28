package com.damavis.spark.resource

import org.apache.spark.sql.DataFrame

trait ResourceReader {
  def read(): DataFrame
}
