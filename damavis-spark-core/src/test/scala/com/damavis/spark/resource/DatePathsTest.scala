package com.damavis.spark.resource

import java.time.LocalDate

import com.damavis.spark.fs.FileSystem
import org.scalatest.WordSpec
import org.scalamock.scalatest.MockFactory

class DatePathsTest extends WordSpec with MockFactory {

  "DatePaths.generate" when {
    "dates are in the proper order" should {
      "return a proper list" in {
        val from = LocalDate.of(2020, 5, 12)
        val to = LocalDate.of(2020, 5, 15)

        implicit val fsStub = stub[FileSystem]
        (fsStub.pathExists _).when(*).returns(true)

        val expected = "basePath/year=2020/month=5/day=12" ::
          "basePath/year=2020/month=5/day=13" ::
          "basePath/year=2020/month=5/day=14" ::
          "basePath/year=2020/month=5/day=15" ::
          Nil

        val generated = DatePaths.generate("basePath", from, to)

        assert(expected === generated)
      }
    }

    "dates are reversed" should {
      "reverse the dates and return a proper list" in {
        val from = LocalDate.of(2020, 5, 15)
        val to = LocalDate.of(2020, 5, 12)

        implicit val fsStub = stub[FileSystem]
        (fsStub.pathExists _).when(*).returns(true)

        val expected = "basePath/year=2020/month=5/day=12" ::
          "basePath/year=2020/month=5/day=13" ::
          "basePath/year=2020/month=5/day=14" ::
          "basePath/year=2020/month=5/day=15" ::
          Nil

        val generated = DatePaths.generate("basePath", from, to)

        assert(expected === generated)
      }
    }

    "dates are the same" should {
      "return a list with a single element" in {
        val from = LocalDate.of(2020, 5, 15)
        val to = LocalDate.of(2020, 5, 15)

        implicit val fsStub = stub[FileSystem]
        (fsStub.pathExists _).when(*).returns(true)

        val expected = "basePath/year=2020/month=5/day=15" ::
          Nil

        val generated = DatePaths.generate("basePath", from, to)

        assert(expected === generated)
      }
    }

    "some paths do not exist" should {
      "return a list with only the existing paths" in {
        val from = LocalDate.of(2020, 5, 12)
        val to = LocalDate.of(2020, 5, 15)

        implicit val fsStub = stub[FileSystem]
        (fsStub.pathExists _)
          .when("basePath/year=2020/month=5/day=12")
          .returns(false)
        (fsStub.pathExists _)
          .when("basePath/year=2020/month=5/day=13")
          .returns(true)
        (fsStub.pathExists _)
          .when("basePath/year=2020/month=5/day=14")
          .returns(true)
        (fsStub.pathExists _)
          .when("basePath/year=2020/month=5/day=15")
          .returns(false)

        val expected = "basePath/year=2020/month=5/day=13" ::
          "basePath/year=2020/month=5/day=14" ::
          Nil

        val generated = DatePaths.generate("basePath", from, to)

        assert(expected === generated)
      }
    }
  }

}
