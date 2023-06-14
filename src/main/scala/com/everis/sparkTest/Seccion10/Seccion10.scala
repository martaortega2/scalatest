package com.everis.sparkTest.Seccion10

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Seccion10 {
  def main(args: Array[String]) = {
    implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //    val df = List(1, 2, 3, 4).toDF("id")
    //    df.show(false)

    val usuarios_df = spark.read.format("csv").option("header", "true").load("src/main/scala/com/everis/sparkTest/Seccion10/usuarios.csv")
    println("TABLA USUARIOS")
    usuarios_df.show(false)

    val compras_df = spark.read.format("csv").option("header", "true").load("src/main/scala/com/everis/sparkTest/Seccion10/compras.csv")
    println("TABLA COMPRAS")
    compras_df.show(false)

    val productos_df = spark.read.format("csv").option("header", "true").load("src/main/scala/com/everis/sparkTest/Seccion10/productos.csv")
    println("TABLA PRODUCTOS")
    productos_df.show(false)

    val envios_df = spark.read.format("csv").option("header", "true").load("src/main/scala/com/everis/sparkTest/Seccion10/envios.csv")
    println("TABLA ENVIOS")
    envios_df.show(false)

    // Total de compras realizadas por país y mes
    val compras_por_pais_y_mes = compras_df
      .join(usuarios_df, compras_df("usuario_id") === usuarios_df("usuario_id"))
      .groupBy(usuarios_df("pais"), month(compras_df("fecha_compra")).alias("mes"))
      .agg(count(compras_df("compra_id")).alias("total_compras"))
      .orderBy(usuarios_df("pais"), month(compras_df("fecha_compra")))
    println("COMPRAS POR PAIS Y MES")
    compras_por_pais_y_mes.show(false)

    // Monto promedio de las compras por usuario y país
    val monto_promedio_compras = compras_df
      .join(usuarios_df, compras_df("usuario_id") === usuarios_df("usuario_id"))
      .groupBy(usuarios_df("usuario_id"), usuarios_df("pais"))
      .agg(avg(compras_df("monto")).alias("monto_promedio"))
    println("MONTO PROMEDIO COMPRAS")
    monto_promedio_compras.show(false)

    // Número de compras realizadas por usuario en un país determinado
    val compras_por_usuario_pais = compras_df
      .join(usuarios_df, compras_df("usuario_id") === usuarios_df("usuario_id"))
      .groupBy(usuarios_df("usuario_id"), usuarios_df("pais"), usuarios_df("nombre"))
      .agg(count(compras_df("compra_id")).alias("numero_compras"))
    println("COMPRAS POR USUARIO Y PAIS")
    compras_por_usuario_pais.show(false)

    // Total de envíos realizados por estado y mes
    val envios_por_estado_y_mes = envios_df
      .groupBy(envios_df("estado"), month(envios_df("fecha_envio")).alias("mes"))
      .agg(count(envios_df("envio_id")).alias("total_envios"))
      .orderBy(envios_df("estado"), month(envios_df("fecha_envio")))
    println("ENVIOS POR ESTADO Y MES")
    envios_por_estado_y_mes.show(false)

    // Monto total de las compras que han sido enviadas y están en estado "Entregado"
    val monto_total_compras_entregadas = compras_df
      .join(envios_df, compras_df("compra_id") === envios_df("compra_id"))
      .filter(envios_df("estado") === "Entregado")
      .agg(sum(compras_df("monto")).alias("monto_total_entregado"))
    println("MONTO TOTAL COMPRAS ENTREGADAS")
    monto_total_compras_entregadas.show(false)

    // Productos más vendidos por mes
    val productos_mas_vendidos_por_mes = compras_df
      .join(productos_df, compras_df("compra_id") === productos_df("producto_id"))
      .groupBy(month(compras_df("fecha_compra")).alias("mes"), productos_df("nombre"))
      .agg(sum(compras_df("monto")).alias("cantidad_total_vendida"))
      .orderBy(col("mes"), desc("cantidad_total_vendida"))
    println("PRODUCTOS MAS VENDIDOS POR MES")
    productos_mas_vendidos_por_mes.show(false)
  }
}
