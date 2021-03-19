package com.damavis.spark.resource.partitioning

import com.damavis.spark.fs.FileSystem
import org.scalamock.scalatest.MockFactory
import org.scalatest.WordSpec

import java.time.LocalDateTime

object DatePartitionsTest {
  val MAY_TWELFTH = "year=2020/month=05/day=12"
  val MAY_THIRTEEN = "year=2020/month=05/day=13"
  val MAY_FOURTEEN = "year=2020/month=05/day=14"
  val MAY_FIFTEEN = "year=2020/month=05/day=15"

  val EXPECTED_DAYS_OF_MAY: Seq[String] = MAY_TWELFTH ::
    MAY_THIRTEEN ::
    MAY_FOURTEEN ::
    MAY_FIFTEEN ::
    Nil
}

class DatePartitionsTest extends WordSpec with MockFactory {
  import DatePartitionsTest._

  "DatePartitions.generate" when {
    "dates are in the proper order" should {
      "return a proper list" in {
        val from = LocalDateTime.of(2020, 5, 12, 0, 0)
        val to = LocalDateTime.of(2020, 5, 15, 0, 0)

        val fsStub = stub[FileSystem]
        (fsStub.pathExists _).when(MAY_TWELFTH).returns(true)
        (fsStub.pathExists _).when(MAY_THIRTEEN).returns(true)
        (fsStub.pathExists _).when(MAY_FOURTEEN).returns(true)
        (fsStub.pathExists _).when(MAY_FIFTEEN).returns(true)
        (fsStub.listSubdirectories _).when(*).returns("year=2020" :: Nil)

        val expected = EXPECTED_DAYS_OF_MAY

        val generated = DatePartitions(fsStub, DatePartitionFormatter.standard)
          .generatePaths(from, to)

        assert(expected === generated)
      }
    }

    "dates are reversed" should {
      "reverse the dates and return a proper list" in {
        val from = LocalDateTime.of(2020, 5, 15, 0, 0)
        val to = LocalDateTime.of(2020, 5, 12, 0, 0)

        val fsStub = stub[FileSystem]
        (fsStub.pathExists _).when(MAY_TWELFTH).returns(true)
        (fsStub.pathExists _).when(MAY_THIRTEEN).returns(true)
        (fsStub.pathExists _).when(MAY_FOURTEEN).returns(true)
        (fsStub.pathExists _).when(MAY_FIFTEEN).returns(true)
        (fsStub.listSubdirectories _).when(*).returns("year=2020" :: Nil)

        val expected = EXPECTED_DAYS_OF_MAY

        val generated = DatePartitions(fsStub, DatePartitionFormatter.standard)
          .generatePaths(from, to)

        assert(expected === generated)
      }
    }

    "dates are the same" should {
      "return a list with a single element" in {
        val from = LocalDateTime.of(2020, 5, 15, 0, 0)
        val to = LocalDateTime.of(2020, 5, 15, 0, 0)

        val fsStub = stub[FileSystem]
        (fsStub.pathExists _).when(MAY_FIFTEEN).returns(true)
        (fsStub.listSubdirectories _).when(*).returns("year=2020" :: Nil)

        val expected = MAY_FIFTEEN ::
          Nil

        val generated = DatePartitions(fsStub, DatePartitionFormatter.standard)
          .generatePaths(from, to)

        assert(expected === generated)
      }
    }

    "some paths do not exist" should {
      "return a list with only the existing paths" in {
        val from = LocalDateTime.of(2020, 5, 12, 0, 0)
        val to = LocalDateTime.of(2020, 5, 15, 0, 0)

        val fsStub = stub[FileSystem]
        (fsStub.pathExists _)
          .when(MAY_TWELFTH)
          .returns(false)
        (fsStub.pathExists _)
          .when(MAY_THIRTEEN)
          .returns(true)
        (fsStub.pathExists _)
          .when(MAY_FOURTEEN)
          .returns(true)
        (fsStub.pathExists _)
          .when(MAY_FIFTEEN)
          .returns(false)
        (fsStub.listSubdirectories _).when(*).returns("year=2020" :: Nil)

        val expected = MAY_THIRTEEN ::
          MAY_FOURTEEN ::
          Nil

        val generated = DatePartitions(fsStub, DatePartitionFormatter.standard)
          .generatePaths(from, to)

        assert(expected === generated)
      }
    }

    "only a subset of possible partitions may exist" should {
      "query filesystem a minimum number of times" in {
        val from = LocalDateTime.of(2017, 1, 1, 0, 0)
        val to = LocalDateTime.of(2021, 12, 31, 0, 0)

        val fsMock = mock[FileSystem]
        (fsMock.listSubdirectories _)
          .expects(*)
          .returning("year=2018" :: "year=2019" :: Nil)
        (fsMock.pathExists _).expects(*).repeat(365 * 2)

        DatePartitions(fsMock, DatePartitionFormatter.standard)
          .generatePaths(from, to)
      }
    }
  }
}
