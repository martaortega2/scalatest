package com.everis.sparkTest

import org.apache.spark.sql.SparkSession
import Seccion8.main
/**
 * El main (en un proyecto Spark) debe incluir solo lo siguiente:
 *      - Lectura de los parametros de entrada (si procede)
 *      - Inicializacion de ficheros de configuracion (si procede)
 *      - Inicializacion de la sesion de Spark
 *      - Inicializar un Logger (opcional)
 *      - Configuraciones de Spark (aplicarlas a la sesion se puede hacer en el spark-submit)
 *      - Orquestación de la ejecución (si procede)
 *      - Llamada al método de trabajo
 *      - Cierre de la sesión de Spark
 */
object Main{

  def main(args: Array[String]): Unit = {

    //Controlar numero de parametros de entrada
    if ( ! (args.length == 1)){
      throw new RuntimeException("Numero de parámetros de entrada incorrecto. Necesario el parametro de fecha en formato yyyymmdd")
    } else{
      println("Numero de parámetros correcto")
    }
    println("Nuevo cambio de git")
    val inputDate::Nil = args.toList // Equivale a:  val date = args(0)

//    val odate = yyyymkmddDateToDashed(inputDate)

//    println(s"Fecha de entrada $inputDate y fecha formateada $odate")

//    implicit val ss :SparkSession= initializeSpark()

    //Llamada a nuestro metodo de proceso de informacion

//    ss.stop()

  }
}
