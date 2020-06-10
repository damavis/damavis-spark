package com.damavis.spark.dataflow

object NoStage extends PipelineStage(null) {
  override def compute(): Unit = ()
}
