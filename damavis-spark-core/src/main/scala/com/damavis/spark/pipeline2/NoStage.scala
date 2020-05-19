package com.damavis.spark.pipeline2

object NoStage extends PipelineStage(null) {
  override def compute(): Unit = ()
}
