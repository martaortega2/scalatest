package com.everis.sparkTest
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Seccion3  {

  def modelizacion (empleados:DataFrame, centros:DataFrame, estrucPers:DataFrame):(DataFrame,DataFrame)= {
    val empresaTmp = empleados
      .join(estrucPers, Seq("ID"), "left")
      .select("Cliente", "Nombre", "HireDate", "Office")
      .toDF("Cliente","NombreEmpleado","HireDate","idcent")

    // Con regexp_replace
    val empresa = empresaTmp
      .join(centros, Seq("idcent"), "left")
      .withColumn("NombreEmpleado", regexp_replace(col("NombreEmpleado"), ".", "\\*"))
      .select("Cliente", "idcent", "NombreEmpleado", "HireDate")
      .toDF("Cliente", "Centro", "NombreEmpleado", "HireDate")

    (empresa.filter(col("Cliente") === "BBVA"),
    empresa.filter(col("Cliente") === "Santander Tecnologia"))
  }

  def antiguedad (input : DataFrame)={
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val submittedDateConvert = new Date()
    val submittedAt = dateFormatter.format(submittedDateConvert)
    input.withColumn("Antiguedad",floor((datediff(lit(submittedAt),col("HireDate"))/365)))
  }
}
