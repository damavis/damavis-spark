package com.damavis.spark

import java.time.{LocalDate, LocalDateTime}

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

package object functions {

  def between(date1: LocalDate, date2: LocalDate): Column =
    if (date1 == date2)
      equal(date1)
    else if (date2.isAfter(date1))
      after(date1) && before(date2)
    else
      after(date2) && before(date1)

  def between(date1: LocalDateTime, date2: LocalDateTime): Column = {
    if (date1 == date2)
      equal(date1)
    else if (date2.isAfter(date1))
      after(date1) && before(date2)
    else
      after(date2) && before(date1)
  }

  def equal(date: LocalDate): Column =
    col("year") === lit(date.getYear) &&
      col("month") === lit(date.getMonthValue) &&
      col("day") === lit(date.getDayOfMonth)

  def equal(datetime: LocalDateTime): Column =
    equal(datetime.toLocalDate) && col("hour") === lit(datetime.getHour)

  def after(date: LocalDate): Column =
    col("year") > date.getYear ||
      (col("year") === date.getYear &&
        col("month") > date.getMonthValue) ||
      (col("year") === date.getYear &&
        col("month") === date.getMonthValue &&
        col("day") >= date.getDayOfMonth)

  def after(date: LocalDateTime): Column =
    col("year") > date.getYear ||
      (col("year") === date.getYear &&
        col("month") > date.getMonthValue) ||
      (col("year") === date.getYear &&
        col("month") === date.getMonthValue &&
        col("day") > date.getDayOfMonth) ||
      (col("year") === date.getYear &&
        col("month") === date.getMonthValue &&
        col("day") === date.getDayOfMonth &&
        col("hour") >= date.getHour)

  def before(date: LocalDate): Column =
    col("year") < date.getYear ||
      (col("year") === date.getYear &&
        col("month") < date.getMonthValue) ||
      (col("year") === date.getYear &&
        col("month") === date.getMonthValue &&
        col("day") <= date.getDayOfMonth)

  def before(date: LocalDateTime): Column =
    col("year") < date.getYear ||
      (col("year") === date.getYear &&
        col("month") < date.getMonthValue) ||
      (col("year") === date.getYear &&
        col("month") === date.getMonthValue &&
        col("day") < date.getDayOfMonth) ||
      (col("year") === date.getYear &&
        col("month") === date.getMonthValue &&
        col("day") === date.getDayOfMonth &&
        col("hour") <= date.getHour)

}
