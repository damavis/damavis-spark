package com.damavis.spark.pipeline2

import org.apache.spark.sql.DataFrame

abstract class PipelineSource {

  def ->(next: PipelineStage): PipelineDefinition = {
    PipelineDefinition(this, next :: Nil)
  }

  def get(): DataFrame
}
