package com.everis.sparkTest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Seccion7 {
  def main(args: Array[String]) = {
    implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

//    val df = List(1, 2, 3, 4).toDF("id")
//    df.show(false)

  }
}
