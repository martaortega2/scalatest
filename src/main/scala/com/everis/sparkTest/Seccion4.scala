package com.everis.sparkTest

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType}


object Seccion4 {

  def loadLocalFileToDataFrame (resourcePath: String, schema: List[String]) (implicit ss: SparkSession): DataFrame = {
    ss
      .read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .load(resourcePath)
      .toDF(schema: _*)
  }

  def modelAgg(empleados:DataFrame)={
    (empleados.groupBy("Genero").count(),
      empleados.groupBy("Departamento").count()
        .withColumn("Percent", col("count")/lit(empleados.count())))
  }

  def formateoCampo (tipo:DataType) :UserDefinedFunction= udf {(campo:Any)=>
    val value = campo.toString
    tipo match{
      case IntegerType |  DoubleType =>{
        if(value.equals("0.0") | value.equals("0")){
          value
        }
        else{
          val sign = value.substring(0, 1) match {
            case "-" => "+"
            case _ => "-"
          }
          val abs = value.substring(0,1) match{
            case "-" => value.substring(1, value.length)
            case _ => value
          }
          sign.concat(abs.replace(".",","))}
      }
      case StringType => value.toUpperCase
      case _ => value
    }
  }

  def formatDF (df: DataFrame) : DataFrame = {
    val columnasTipos  = df.schema.toList.map(x=>(x.name,x.dataType))
    columnasTipos.foldLeft(df)((acc,x)=>acc.withColumn(x._1,formateoCampo(x._2)(col(x._1))))
  }

  def formatDF2 (df: DataFrame) : DataFrame = {
    val columnasTipos  = df.schema.toList.map(x=>(x.name,x.dataType))
    columnasTipos.foldLeft(df)((acc,x)=>acc.withColumn(x._1,x._2 match{
      case IntegerType |  DoubleType =>
        when(col(x._1).isin(0,0.0),col(x._1))
          .otherwise(regexp_replace(lit(-1)*col(x._1),"\\.",","))
      case _ => upper(col(x._1))
    }))
  }

  def formatDF3 (df: DataFrame) : DataFrame = {
    val columnasTipos  = df.schema.toList.map(x=>(x.name,x.dataType))
    var df2 = df
    for (c <- columnasTipos){
      df2 = df2.withColumn(c._1,
      c._2 match {
        case IntegerType | DoubleType =>
          when(col(c._1).isin(0, 0.0), col(c._1))
            .otherwise(regexp_replace(lit(-1) * col(c._1), "\\.", ","))
        case _ =>
          upper(col(c._1))
        })
    }
    df2
  }
}
