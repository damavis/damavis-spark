package com.damavis.spark.pipeline3

object NoStage extends PipelineStage(null) {
  override def compute(): Unit = ()
}
