package com.damavis.spark.resource.datasource

object OverwritePartitionBehavior extends Enumeration {
  type OverwritePartitionBehavior = Value

  /*The following enum controls the behavior of the configuration parameter "partitionOverwriteMode" */
  val OVERWRITE_ALL, OVERWRITE_MATCHING = Value
}
