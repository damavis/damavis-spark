package com.damavis.spark.dataflow

class PipelineSource(processor: SourceProcessor)
    extends PipelineStage(processor) {

  override def ->(stage: StageSocket)(
      implicit definition: PipelineDefinition): PipelineStage = {
    definition.addSource(this)

    super.->(stage)
  }

}
