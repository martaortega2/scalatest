package com.everis.sparkTest

import org.apache.spark.sql.SparkSession

object ATestear {

  def lecturaDataFrame(path:String)(implicit ss:SparkSession)={
    ss.read.option("delimiter",";").option("header","true").option("inferSchema","true").format("csv").load(path)//.toDF(cols:_*)
  }


  def lecturaSegura(path:String)(implicit ss:SparkSession)={
    try{
      lecturaDataFrame(path)
    } catch{
      case e:Exception=>{println("ha petado")
        throw e}
    }
  }
}
