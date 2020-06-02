package com.damavis.spark.pipeline

import org.apache.spark.sql.DataFrame

/**
  * Pipeline target, this trait defines the interface of
  * what to do with a DataFrame.
  */
trait PipelineTarget extends PipelineStage {

  override def transform(data: DataFrame): DataFrame = {
    put(data)
    data
  }

  def put(data: DataFrame): Unit
}
