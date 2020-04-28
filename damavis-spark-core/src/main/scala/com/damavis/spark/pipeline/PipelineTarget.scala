package com.damavis.spark.pipeline

import org.apache.spark.sql.DataFrame

/**
  * Pipeline target, this trait defines the interface of
  * what to do with a DataFrame.
  */
trait PipelineTarget {
  def put(data: DataFrame): Unit
}
