package com.damavis.spark.pipeline

import org.apache.spark.sql.DataFrame

/**
  * Pipeline Stage trait defines simple and testable interface.
  */
trait PipelineStage {

  /**
    * Transforms the DataFrame.
    * @param data    The DataFrame.
    * @return        The Transformed DataFrame.
    */
  def transform(data: DataFrame): DataFrame

  /**
    * Operator to create Pipelines from Stages, this is combined to
    * the same method on [[Pipeline]] to concatenate stages and pipelines.
    * @param stage   A PipelineStage
    * @return        A Pipeline
    */
  def ->(stage: PipelineStage): Pipeline = this -> Pipeline(stage)

  /**
    * Operator to create Pipelines from Stage and Pipeline, this is
    * combined to the same method on [[Pipeline]] to concatenate stages
    * and pipelines
    * @param pipeline  A Pipeline
    * @return          A Pipeline
    */
  def ->(pipeline: Pipeline): Pipeline = Pipeline(this :: pipeline.getStages)
}
