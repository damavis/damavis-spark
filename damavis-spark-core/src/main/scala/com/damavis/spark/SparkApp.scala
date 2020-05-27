package com.damavis.spark

import org.apache.spark.sql.SparkSession

trait SparkApp extends SparkConf {

  val name: String

  private var mutableSession: SparkSession = _
  lazy val session: SparkSession = spark

  implicit def spark: SparkSession = {
    if (mutableSession == null) {
      val spark = SparkSession.builder().appName(name)
      val sparkWithMaster = {
        sys.env.get("MASTER") match {
          case Some(master) => spark.master(master)
          case _            => spark
        }
      }
      val configuredSpark = conf.foldLeft(sparkWithMaster) {
        (instance, keyVal) =>
          instance.config(keyVal._1, keyVal._2)
      }

      mutableSession = configuredSpark
        .enableHiveSupport()
        .getOrCreate()
    }

    mutableSession
  }

  /**
    * Get the number of executors running in cluster. It's useful for
    * optimize coalesce writing. If master is local[?] number of cores
    * will be used instead.
    * @return  The number of executors.
    */
  def getExecutorsNum: Int = {
    val executors = spark.sparkContext.getExecutorMemoryStatus.size - 1
    if (executors > 0) executors
    else {
      val master = spark.conf.get("spark.master")
      val localCores = "local\\[(\\d+|\\*)\\]".r.findAllIn(master)
      if (localCores.hasNext) localCores.group(1) match {
        case "*" => sys.runtime.availableProcessors()
        case x   => x.toInt
      } else {
        1
      }
    }
  }

}
