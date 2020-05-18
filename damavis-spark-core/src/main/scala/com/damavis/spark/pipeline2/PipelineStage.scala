package com.damavis.spark.pipeline2

import org.apache.spark.sql.DataFrame

trait PipelineStage {

  def compute(data: DataFrame): DataFrame
}
