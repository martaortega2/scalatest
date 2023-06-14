package com.everis.sparkTest

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Seccion6 {
  /**
   * 1. Escribir una funcion que emplee como parametro de entrada una lista de tipo string y
   *    realice calculo sobre las mismas y que capturar el error que de al pasarle una lista
   *    de tipo Any
   * */
  def upperList (list:List[String]): List[String]={
    list.map(x=>x.toUpperCase)
  }

  def listTry (lista:List[String])= {
    try {
      upperList(lista.asInstanceOf[List[String]])
    }
    catch {
      case e: Error => {
        println("He capturado la excepcion 1")
        throw e
      }
      case f: Exception => {
        println("He capturado la excepcion 2")
        throw f
      }
    }
  }

  /**
   * 2. Generar una funcion que reciba como parametro de entrada un tipo Option[String] y en caso de recibir valor lo
   *    desempaquete y en otro caso, devuelva una cadena vacia
   *    Realizar la misma operacion con un Int y un cero
   * */
  def unapplyOpt (valor:Option[String]):String={
    valor match{
      case Some(x)=>x
      case _ => ""
    }
  }

  def unapplyOpt (valor:Option[Int]):Int={
    valor match{
      case Some(x)=>x
      case _ => 0
    }
  }

  /**
   *   3. Empleando la funcion loadLocalFileToDataFrame ya generada en otras sesiones, reescribirla
   *      para hacerla robusta frente a errores.
   * */
  def loadLocalFileToDataFrame(resourcePath: String, schema: List[String]) (implicit ss:SparkSession): DataFrame = {
    ss
      .read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .load(resourcePath)
      .toDF(schema: _*)
  }

  def lecturaEmpleado(resourcePath: String, schema: List[String])(implicit ss: SparkSession):DataFrame={
    import ss.implicits._
    try{
      loadLocalFileToDataFrame(resourcePath, schema)
    }
    catch{
      case e:java.lang.IllegalArgumentException => {
        println("*******************************************")
        println(" te has equivocado en el schema, corrigelo")
        println("*******************************************")
        throw e
      }
      case f:org.apache.spark.sql.AnalysisException =>{
        println("******************************************************************")
        println("HA FALLADO LA LECTURA DE EMPLEADOS por la ruta, se devuelve vacio")
        println("******************************************************************")
        List.empty[String].toDF("Id")

      }
    }
  }

  /**
   * 4. Empleando la funcion de formateo de valores, reescribiendo la funcion como una udf, emplear el tipo Option
   * para la imputacion de valores ausentes
   *
   * */
  def formateoCampo (tipo:DataType) :UserDefinedFunction= udf { campo: Any =>
    val optVal = Option(campo)
    optVal match {
      case Some(valor) =>
        val value = valor.toString
        tipo match {
          case IntegerType | DoubleType =>
            if (value.equals("0.0") | value.equals("0")) {
              value
            }
            else {
              val sign = value.substring(0, 1) match {
                case "-" => "+"
                case _ => "-"
              }
              val abs = value.substring(0, 1) match {
                case "-" => value.substring(1, value.length)
                case _ => value
              }
              sign.concat(abs.replace(".", ","))
            }

          case StringType => value.toUpperCase
          case _ => value
        }

      case None =>
        tipo match {
          case IntegerType | DoubleType => "0"
          case _ => ""
        }
    }
  }

  def formatDF (df: DataFrame) : DataFrame = {
    val columnasTipos  = df.schema.toList.map(x=>(x.name,x.dataType))
    columnasTipos.foldLeft(df)((acc,x)=>acc.withColumn(x._1,formateoCampo(x._2)(col(x._1))))
  }

}
