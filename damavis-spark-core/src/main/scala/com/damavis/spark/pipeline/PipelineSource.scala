package com.damavis.spark.pipeline

import org.apache.spark.sql.DataFrame

/**
  * Pipeline source, this trait defines the interface to obtain a DataFrame.
  */
trait PipelineSource {
  def get: DataFrame
  def |(pipeline: Pipeline): DataFrame = pipeline.transform(get)
}
