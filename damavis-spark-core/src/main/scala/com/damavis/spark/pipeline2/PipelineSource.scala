package com.damavis.spark.pipeline2

class PipelineSource(processor: Processor) extends PipelineStage(processor) {

  override def ->(stage: StageSocket)(
      implicit definition: PipelineDefinition): PipelineStage = {
    definition.addSource(this)

    super.->(stage)
  }

}
