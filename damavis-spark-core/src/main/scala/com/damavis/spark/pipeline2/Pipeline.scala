package com.damavis.spark.pipeline2

class Pipeline(definition: PipelineDefinition) {

  def run(): Unit = {
    val initialData = definition.source.get()
    val finalData =
      definition.stages.foldLeft(initialData)((df, stage) => stage.compute(df))

    definition.target.put(finalData)
  }

}
