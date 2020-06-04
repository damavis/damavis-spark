package com.damavis.spark.dataflow

class Pipeline(definition: PipelineDefinition) {
  def run(): Unit = {
    for (source <- definition.sources)
      source.compute()
  }

}
