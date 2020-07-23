package com.damavis.spark.dataflow

class DataFlowSource(processor: SourceProcessor)
    extends DataFlowStage(processor) {

  override def ->(stage: StageSocket)(
      implicit definition: DataFlowDefinition): DataFlowStage = {
    definition.addSource(this)

    super.->(stage)
  }

}
