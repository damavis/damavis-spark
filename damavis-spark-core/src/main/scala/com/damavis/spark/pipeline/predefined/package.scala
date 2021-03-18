package com.damavis.spark.pipeline

package object predefined {
  def cache: PipelineStage = new CacheStage
}
