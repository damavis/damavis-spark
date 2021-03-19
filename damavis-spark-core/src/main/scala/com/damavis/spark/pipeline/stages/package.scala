package com.damavis.spark.pipeline

package object stages {
  def cache: PipelineStage = new CacheStage
}
