package com.damavis.spark.dataflow

class PipelineDefinition() {
  var sources: List[PipelineStage] = Nil

  def addSource(source: PipelineStage): Unit = {
    sources = source :: sources
  }

}
