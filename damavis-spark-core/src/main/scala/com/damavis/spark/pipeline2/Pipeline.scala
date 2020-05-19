package com.damavis.spark.pipeline2

class Pipeline(definition: PipelineDefinition) {
  def run(): Unit = {
    for (source <- definition.sources)
      source.compute()
  }

}
