package com.everis.sparkTest.Seccion11

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Seccion11Mejor {
  def main(args: Array[String]) = {
    implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    // Cargar los datos en DataFrames
    val usuariosDF = spark.read.option("header", "false").csv("src/main/scala/com/everis/sparkTest/Seccion11/usuarios.csv").toDF("id_usuario", "nombre", "fecha_registro")
    val ventasDF = spark.read.option("header", "false").csv("src/main/scala/com/everis/sparkTest/Seccion11/ventas.csv").toDF("id_venta", "id_usuario", "fecha_venta", "monto_total")
    val comentariosDF = spark.read.option("header", "false").csv("src/main/scala/com/everis/sparkTest/Seccion11/comentarios.csv").toDF("id_comentario", "id_usuario", "fecha_comentario", "contenido")

    val montoVentasPorUsuarioDF = ventasDF.groupBy("id_usuario").agg(sum("monto_total").as("monto_total_ventas"))

    val usuarioUltimoComentarioDF = comentariosDF.groupBy("id_usuario")
      .agg(max("fecha_comentario").as("fecha_comentario"))
      .join(comentariosDF, Seq("id_usuario", "fecha_comentario"))
      .select("id_usuario", "contenido")

    val promedioVentasPorMesDF = ventasDF.withColumn("mes", date_format(to_date(col("fecha_venta")), "yyyy-MM"))
      .groupBy("mes")
      .agg(avg("monto_total").as("promedio_montos_ventas"))

    val sumaMontosVentasPorMesUsuarioDF = ventasDF.withColumn("mes", date_format(to_date(col("fecha_venta")), "yyyy-MM"))
      .groupBy("mes", "id_usuario")
      .agg(sum("monto_total").as("suma_montos_ventas"))

    val usuarioVentaMayorMontoDF = ventasDF.join(ventasDF.groupBy("id_usuario").agg(max("monto_total").as("monto_total_maximo")), Seq("id_usuario", "monto_total"))
      .select("id_usuario", "monto_total")

    val cantidadComentariosPorUsuarioDF = comentariosDF.groupBy("id_usuario").agg(count("*").as("cantidad_comentarios"))

    val mesMayorCantidadComentariosDF = comentariosDF.withColumn("mes", date_format(to_date(col("fecha_comentario")), "yyyy-MM"))
      .groupBy("mes")
      .agg(count("*").as("cantidad_comentarios"))
      .orderBy(desc("cantidad_comentarios"))
      .limit(1)

    val diferenciaDiasRegistroVentaDF = usuariosDF.join(ventasDF, "id_usuario")
      .withColumn("diferencia_dias", datediff(to_date(col("fecha_venta")), to_date(col("fecha_registro"))))
      .select("id_usuario", "diferencia_dias")

    val ventasPorProductoDF = ventasDF.groupBy("id_venta").agg(count("*").as("cantidad_ventas"))

    val productoMasVendidoDF = ventasDF.groupBy("id_venta").agg(count("*").as("cantidad_ventas"))
      .orderBy(desc("cantidad_ventas"))
      .limit(1)

//    val sumaMontosVentasPorCategoriaDF = ventasDF.join(categoriasDF, Seq("id_categoria"))
//      .groupBy("nombre_categoria")
//      .agg(sum("monto_total").as("suma_montos_ventas"))
//
//    val categoriaMayorCantidadVentasDF = ventasDF.join(categoriasDF, Seq("id_categoria"))
//      .groupBy("nombre_categoria")
//      .agg(count("*").as("cantidad_ventas"))
//      .orderBy(desc("cantidad_ventas"))
//      .limit(1)


  }
}
