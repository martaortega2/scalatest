package com.everis.sparkTest

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}

object Seccion8 {
  def main(args:Array[String]):Unit={
    //Controlar numero de parametros de entrada
    if ( ! (args.length == 1)){
      throw new RuntimeException("Numero de parámetros de entrada incorrecto. Necesario el parametro de fecha en formato yyyymmdd")
    } else{
      println("Numero de parámetros correcto")
    }

    val inputDate::Nil = args.toList // Equivale a:  val date = args(0)

    val fmt_yyyyMMdd: DateTimeFormatter = DateTimeFormat forPattern "yyyyMMdd"
    val fmt_yyyy_MM_dd: DateTimeFormatter = DateTimeFormat forPattern "yyyy-MM-dd"

    def yyyymmddDateToDashed(dateyyyymmdd: String): String = fmt_yyyy_MM_dd print(fmt_yyyyMMdd parseDateTime dateyyyymmdd)

    val odate = yyyymmddDateToDashed(inputDate)

    println(s"Fecha de entrada $inputDate y fecha formateada $odate")

    implicit val ss :SparkSession= SparkSession
      .builder()
      .master("local")
      .appName("Program Execution Driver")
      .getOrCreate()

    //Llamada a nuestro metodo de proceso de informacion

    modelizacion(odate)

    ss.stop()
  }


  def lecturaDataFrame(path:String)(implicit ss:SparkSession):DataFrame={
    ss.read
      .option("delimiter",";")
      .option("header","false")
      .option("inferSchema","true")
      .csv(path)//.toDF(cols:_*)
  }

  def lecturaDataFrameStrings(path:String)(implicit ss:SparkSession):DataFrame={
    ss.read
      .option("delimiter",";")
      .option("header","false")
      .option("inferSchema","false")
      .csv(path)//.toDF(cols:_*)
  }

  def generacionNumericSituac (situacion:Column):Column={
    when(situacion==="FAL",lit(2)).otherwise(when(situacion==="MOR",lit(1)).otherwise(lit(0)))
  }

  def modelizacion(date:String)(implicit ss:SparkSession):DataFrame={
    //Parametros de entrada y definición de ventanas
//    val inputPath="file:///C:/Users/jnunezdo/Everis/Pruebas/DataVaultWSS/"
    val inputPath="file:///C:/Users/mortegom/Downloads/ProyectoFinalWS2023"
    val winSituacs = Window.partitionBy("idcontr").orderBy(desc("fecfin"))
    val winSitNum = Window.partitionBy("idpers").orderBy(desc("num_sit"))

    //Extraccion de Ficheros de Entrada
    val tipos_segmento_COM = lecturaDataFrame(inputPath +"segmentos-tipos_segmentos.csv").toDF("idseg","tiposeg")
      .filter(col("tiposeg")==="COM")

    val link_personas_segmentos = lecturaDataFrame(inputPath +"link-personas_segmentos.csv").toDF("idpers","idseg","fecalta","fecbaja")
      .withColumn("fecalta", substring(col("fecalta"),1,10))
      .withColumn("fecbaja", substring(col("fecbaja"),1,10))
      .filter(col("fecalta")<=date && col("fecbaja")>date)

    val posicionContratos = lecturaDataFrameStrings(inputPath +"posicion_contratos.csv").toDF("idcontr","tipmvto","importe")
      .withColumn("importe", regexp_replace(col("importe"),",","\\.").cast("double"))

    val situacionContratos = lecturaDataFrame(inputPath +"situacion_contratos.csv").toDF("idcontr","situac","fecini","fecfin")

    val link_personas_contratos = lecturaDataFrame(inputPath +"link-personas_contratos.csv").toDF("idpers","idcontr","fecalta")
      .withColumn("fecalta", substring(col("fecalta"),1,10))
      .filter(col("fecalta")<=date)

    val personas = lecturaDataFrame(inputPath +"personas.csv").toDF("idpers")
    val personasEstr  = lecturaDataFrame(inputPath +"personas-estructural.csv").toDF("idpers","numtlf","direccion","numtlf")

    //Preproceso y Tablas Auxiliares
    val posicionContratosInf = posicionContratos
      .groupBy("idcontr").agg(sum(col("importe")).alias("importe"),sum(abs(col("importe"))).alias("posicion"))
      .filter(col("posicion")=!=0).select("idcontr","posicion","importe")

    val peorSit = link_personas_contratos.join(situacionContratos,Seq("idcontr"),"inner").withColumn("num_sit", generacionNumericSituac(col("situac")))
      .withColumn("row",row_number() over winSitNum)
      .filter(col("row")===1).drop("row").select("idpers","situac").toDF("idpers","peorsit")

    val importePers = link_personas_contratos.join(posicionContratos,Seq("idcontr"),"inner")
      .groupBy("idpers").sum("importe").toDF("idpers","importe")

    val situacionContratosIr = situacionContratos
      .filter(col("situac").isin("IR","MOR","FAL"))
      .withColumn("fecini", substring(col("fecini"),1,10))
      .withColumn("fecfin", substring(col("fecfin"),1,10))
      .withColumn("row_num",row_number() over winSituacs )
      .filter(col("row_num")===1).drop("row_num").select("idcontr","situac").toDF("idcontr","situ_actual")

    //Modelizacion
    val scope_segmentos = link_personas_segmentos
      .join(tipos_segmento_COM, Seq("idseg"),"inner")
      .select("idseg","idpers")

    val scope_contratos = link_personas_contratos
      .join(posicionContratosInf, Seq("idcontr"),"inner")
      .join(situacionContratosIr, Seq("idcontr"),"inner")

    val proyeccionPers = scope_contratos.select("idpers").dropDuplicates()
      .join(peorSit, Seq("idpers"),"left")
      .join(importePers,Seq("idpers"),"left")
      .select("idpers","importe","peorsit")

    val outputDF = personas.join(personasEstr,Seq("idpers"),"left")
      .join(scope_segmentos, Seq("idpers"),"inner")
      .join(proyeccionPers, Seq("idpers"),"inner")

    outputDF
  }
}

