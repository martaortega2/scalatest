package com.everis.sparkTest

import org.apache.spark.sql.SparkSession

object Fin {
  def main (args:Array[String])= {
    implicit val ss = SparkSession.builder().master("local[*]").getOrCreate()
    import ss.implicits._


  }

}
