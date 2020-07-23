package com.damavis.spark.dataflow

class DataFlowDefinition() {
  var sources: List[DataFlowStage] = Nil

  def addSource(source: DataFlowStage): Unit = {
    sources = source :: sources
  }

}
