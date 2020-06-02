package com.damavis.spark.pipeline

import org.apache.spark.sql.DataFrame

object Pipeline {
  def apply(stages: List[PipelineStage]): Pipeline = new Pipeline(stages)

  def apply(stage: PipelineStage, stages: PipelineStage*): Pipeline =
    new Pipeline(stage :: stages.toList)

  def apply(pipelines: Pipeline*): Pipeline =
    new Pipeline(pipelines.flatMap(x => x.getStages).toList)
}

class Pipeline(stages: List[PipelineStage]) {
  protected val stagesList: List[PipelineStage] = stages

  def getStages: List[PipelineStage] = stagesList

  def transform(data: DataFrame): DataFrame =
    getStages.foldLeft(data) { (data, stage) =>
      stage.transform(data)
    }

  def ->(stage: PipelineStage): Pipeline =
    Pipeline(this.getStages ++ List(stage))
}
