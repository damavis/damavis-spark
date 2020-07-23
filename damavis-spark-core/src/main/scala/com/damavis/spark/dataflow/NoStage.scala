package com.damavis.spark.dataflow

object NoStage extends DataFlowStage(null) {
  override def compute(): Unit = ()
}
