package com.everis.sparkTest.Seccion12

import org.apache.spark.sql.SparkSession

object Seccion12 {
  def main(args: Array[String]) = {
    implicit val ss = SparkSession.builder().master("local[*]").getOrCreate()
    import ss.implicits._

//    val df = List(1, 2, 3, 4).toDF("id")
//    df.show(false)

  }
}
