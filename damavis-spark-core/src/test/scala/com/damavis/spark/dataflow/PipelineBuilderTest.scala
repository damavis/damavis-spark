package com.damavis.spark.dataflow

import org.scalatest.WordSpec

class PipelineBuilderTest extends WordSpec {

  /*TODO
 * - empty pipelines are not valid
 * - pipelines with no sources are not valid
 * - for every source, all executions paths must lead to a target. Otherwise, pipeline is not valid
 * - pipelines with loops are not valid
 * - pipelines with null pointers along any execution path are not valid
 * - choose: pipelines with unreachable stages are valid? Throw an error or just log it instead?
 * */

}
