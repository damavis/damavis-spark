package com.damavis.spark.dataflow

class DataFlow(definition: DataFlowDefinition) {
  def run(): Unit = {
    for (source <- definition.sources)
      source.compute()
  }

}
