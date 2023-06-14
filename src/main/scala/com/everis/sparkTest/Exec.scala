package com.everis.sparkTest
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
 * Hello world!
 *
 */
object Exec{

  def main(args: Array[String]): Unit = {
    val cfg = new ConfigArgs

    try{
      cfg.parse(args)
      if(cfg.mostrarAyuda){
        cfg.printHelp()
      }
    } catch{
      case e: Exception => {
        println(s"Ha habido un error en la lectura:${e.getMessage}")
        throw e
      }
    }

    val ss = SparkSession
      .builder()
      .appName("Program Execution Driver")
      .getOrCreate()

    def loadLocalFileToDataFrame(resourcePath: String, schema: List[String]) = {
      ss
        .read
        .format("csv")
        .option("header", "false")
        .option("inferSchema", "true")
        .option("delimiter", ";")
        .load(resourcePath)
        .toDF(schema: _*)
    }

    val sc = ss.sparkContext
    val sparkConf = new SparkConf

    val empleado = loadLocalFileToDataFrame(cfg.getFile, cfg.getSchema)
    empleado.show(false)

    val critDpto = ss.table("develop.critic_department").select("department").toDF("Departamento")

    critDpto.show(false)

    val joinEmpDpto = critDpto.join(empleado, Seq("Departamento"),"inner").groupBy("Departamento").count

    joinEmpDpto
      .write
      .mode(SaveMode.Overwrite)
      .insertInto("develop.employees")

  }
}
