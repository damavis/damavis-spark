package com.damavis.spark.pipeline3

class PipelineDefinition() {
  var sources: List[PipelineStage] = Nil

  def addSource(source: PipelineStage): Unit = {
    sources = source :: sources
  }

}
