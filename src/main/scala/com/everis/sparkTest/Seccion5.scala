package com.everis.sparkTest

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Seccion5 {

  def ventanasSimples (df:DataFrame) (implicit ss: SparkSession)={
    import ss.implicits._

    val windowSalary = Window.orderBy("salary").partitionBy("department")
    val windowDept = Window.partitionBy("department")

    df.withColumn("row_num",row_number().over(windowSalary))
      .withColumn("avgSal",avg(col("salary")).over(windowDept))
      .withColumn("totSal",sum(col("salary")).over(windowDept))
      .withColumn("minSal",min(col("salary")).over(windowDept))
      .withColumn("maxSal",max(col("salary")).over(windowDept))
      .filter($"row_num"===1)
  }


  def window2 (dataset:DataFrame) (implicit ss:SparkSession)={
    val windowCatRev = Window.partitionBy("category").orderBy(desc("totalRev"))
    val windowCat = Window.partitionBy("category")
    val datasetCat = dataset.groupBy("category").sum("revenue").toDF("category","totalRev")

    (dataset
        .withColumn("totalRev",sum("revenue").over(windowCat))
        .withColumn("row_num",row_number().over(windowCatRev))
      .filter(col("row_num")<=2),
    datasetCat
      .crossJoin(datasetCat.toDF("categoryComp","totalRevCat"))
      .withColumn("difCat",col("totalRev")-col("totalRevCat")))
  }



}
