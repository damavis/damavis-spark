package com.damavis.spark.dataflow

class DataFlowTarget(processor: Processor) extends DataFlowStage(processor) {

  override def compute(): Unit = processor.compute(sockets)

  override def ->(stage: StageSocket)(
      implicit definition: DataFlowDefinition): DataFlowStage = ???

}
