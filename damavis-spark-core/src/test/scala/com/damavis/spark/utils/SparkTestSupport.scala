package com.damavis.spark.utils

import scala.util.Try

trait SparkTestSupport {

  def checkExceptionOfType[T <: Throwable](tryResult: Try[_],
                                           exClass: Class[T],
                                           pattern: String): Unit = {
    assert(tryResult.isFailure)

    try {
      throw tryResult.failed.get
    } catch {
      case ex: Throwable =>
        assert(ex.getClass == exClass)
        assert(ex.getMessage.contains(pattern))
    }
  }

}
