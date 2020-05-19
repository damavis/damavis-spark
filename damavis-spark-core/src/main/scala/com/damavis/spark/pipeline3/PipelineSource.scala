package com.damavis.spark.pipeline3

class PipelineSource(processor: Processor) extends PipelineStage(processor) {

  override def ->(stage: PipelineStage)(
      implicit definition: PipelineDefinition): PipelineStage = {
    definition.addSource(this)
    super.->(stage)
  }

  override def ->(target: PipelineTarget)(
      implicit definition: PipelineDefinition): Pipeline = {
    definition.addSource(this)
    super.->(target)
  }

}
