package com.everis.sparkTest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object Seccion9 {
  def main(args: Array[String]) = {
    import org.apache.spark.sql.functions._

    // Creamos una sesión de Spark
    implicit val ss = SparkSession.builder().master("local[*]").getOrCreate()
    import ss.implicits._
    //    val spark = SparkSession.builder().appName("Análisis de ventas").getOrCreate()

    // Cargamos los datos de ventas.csv en un dataframe de Spark
    val ventas_df = ss.read.format("csv").option("header", "true").load("src/main/scala/com/everis/sparkTest/ventas.csv")
    println("TABLA DE VENTAS")
    ventas_df.show(false)

    // Cargamos los datos de productos.csv en un dataframe de Spark
    val productos_df = ss.read.format("csv").option("header", "true").load("src/main/scala/com/everis/sparkTest/productos.csv")
    println("TABLA DE PRODUCTOS")
    productos_df.show(false)

    // Calculamos el total de ventas por producto y categoría
    val ventas_por_producto_y_categoria = ventas_df.join(productos_df, "producto").groupBy("categoria", "producto")
      .agg(sum("cantidad").alias("total_ventas"))
    println("TABLA DE VENTAS POR PRODUCTO Y CATEGORIA")
    ventas_por_producto_y_categoria.show(false)

    // Calculamos el promedio de precio unitario por categoría
    val precio_unitario_promedio_por_categoria = ventas_df.join(productos_df, "producto").groupBy("categoria")
      .agg(avg("precio_unitario").alias("precio_unitario_promedio"))
    println("TABLA DE PRECIO UNITARIO PROMEDIO POR CATEGORIA")
    precio_unitario_promedio_por_categoria.show(false)

    // Encontramos el producto más vendido en mayo de 2022
    val producto_mas_vendido = ventas_df.filter("fecha >= '01/05/2022' and fecha <= '31/05/2022'")
      .groupBy("producto").agg(sum("cantidad").alias("total_ventas")).orderBy($"total_ventas".desc).first().getString(0)
    println(s"EL PRODUCTO MAS VENDIDO EN MAYO DE 2022 FUE: $producto_mas_vendido")

    // Encontramos el cliente que más dinero gastó en tod0 el periodo de tiempo registrado
    val cliente_que_mas_gasto = ventas_df.groupBy(col("cliente")).agg(sum((col("cantidad") * col("precio_unitario"))
      .cast(IntegerType)).alias("total_gastado")).orderBy(col("total_gastado").desc).first().getString(0)
    println(s"EL CLIENTE QUE MAS GASTO ES: $cliente_que_mas_gasto")

    // Encontramos cuanto dinero gasto el cliente que más dinero gastó en tod0 el periodo de tiempo registrado
//    val cliente_que_mas_gasto_dinero = ventas_df.groupBy(col("cliente")).agg(sum((col("cantidad") * col("precio_unitario"))
//      .cast(IntegerType)).alias("total_gastado")).orderBy(col("total_gastado").desc).select(col("cliente"), col("total_gastado")).limit(1)
//    println("EL CLIENTE QUE MAS GASTO DINERO Y CUANTO")
//    cliente_que_mas_gasto_dinero.show(false)




    // Guardamos los resultados en un archivo CSV
    //    ventas_por_producto_y_categoria.write.format("csv").option("header", "true")
    //      .mode("overwrite").save("src/main/scala/com/everis/sparkTest/ventas_por_producto_y_categoria.csv")
    ventas_por_producto_y_categoria.coalesce(1).write.format("csv").option("header", "true")
      .mode("overwrite").save("src/main/scala/com/everis/sparkTest/ventas_por_producto_y_categoria.csv")

    precio_unitario_promedio_por_categoria.coalesce(1).write.format("csv").option("header", "true")
      .mode("overwrite").save("src/main/scala/com/everis/sparkTest/precio_unitario_promedio_por_categoria.csv")

    ss.sparkContext.parallelize(Seq(producto_mas_vendido)).toDF("producto_mas_vendido_en_mayo_2022")
      .coalesce(1).write.format("csv").option("header", "true").mode("overwrite")
      .save("src/main/scala/com/everis/sparkTest/producto_mas_vendido_en_mayo_2022.csv")

    ss.sparkContext.parallelize(Seq(cliente_que_mas_gasto)).toDF("cliente_que_mas_gasto_en_todo_el_periodo")
      .coalesce(1).write.format("csv").option("header", "true").mode("overwrite")
      .save("src/main/scala/com/everis/sparkTest/cliente_que_mas_gasto_en_todo_el_periodo.csv")
    ss.close()
  }
}
