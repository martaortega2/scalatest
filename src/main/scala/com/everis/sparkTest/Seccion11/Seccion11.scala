package com.everis.sparkTest.Seccion11

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Seccion11 {
  def main(args: Array[String]) = {
    implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._

    //    val df = List(1, 2, 3, 4).toDF("id")
    //    df.show(false)

    import org.apache.spark.sql.types._

    // Definir el esquema para cada archivo CSV
    val usuariosSchema = StructType(Seq(
      StructField("id_usuario", IntegerType, nullable = false),
      StructField("nombre", StringType, nullable = false),
      StructField("fecha_registro", StringType, nullable = false)
    ))

    val ventasSchema = StructType(Seq(
      StructField("id_venta", IntegerType, nullable = false),
      StructField("id_usuario", IntegerType, nullable = false),
      StructField("fecha_venta", StringType, nullable = false),
      StructField("monto_total", DoubleType, nullable = false)
    ))

    val comentariosSchema = StructType(Seq(
      StructField("id_comentario", IntegerType, nullable = false),
      StructField("id_usuario", IntegerType, nullable = false),
      StructField("fecha_comentario", StringType, nullable = false),
      StructField("comentario", StringType, nullable = false)
    ))

    // Leer los archivos CSV y asignar nombres a los dataframes
    val usuarios_df = spark.read
      .option("header", false)
      .schema(usuariosSchema)
      .csv("src/main/scala/com/everis/sparkTest/Seccion11/usuarios.csv")
      .toDF("id_usuario", "nombre", "fecha_registro")
    println("TABLA USUARIOS")
    usuarios_df.show(false)

    val ventas_df = spark.read
      .option("header", false)
      .schema(ventasSchema)
      .csv("src/main/scala/com/everis/sparkTest/Seccion11/ventas.csv")
      .toDF("id_venta", "id_usuario", "fecha_venta", "monto_total")
    println("TABLA VENTAS")
    ventas_df.show(false)

    val comentarios_df = spark.read
      .option("header", false)
      .schema(comentariosSchema)
      .csv("src/main/scala/com/everis/sparkTest/Seccion11/comentarios.csv")
      .toDF("id_comentario", "id_usuario", "fecha_comentario", "comentario")
    println("TABLA COMENTARIOS")
    comentarios_df.show(false)







//    //    Calcular el monto total de ventas por usuario:
//    val montoTotalVentasPorUsuarioDF = ventas_df.join(usuarios_df, "id_usuario")
//      .groupBy("id_usuario", "nombre")
//      .agg(sum("monto_total").as("monto_total_ventas"))
//    println("MONTO TOTAL VENTAS POR USUARIO")
//    montoTotalVentasPorUsuarioDF.show(false)
//
//    //    Obtener el usuario que ha realizado el comentario más reciente:
//    val usuario_ultimo_comentario = comentarios_df
//      .join(usuarios_df, comentarios_df("id_usuario") === usuarios_df("id_usuario"))
//      .groupBy(usuarios_df("id_usuario"), usuarios_df("nombre"))
//      .agg(max(to_date(comentarios_df("fecha_comentario"))).alias("fecha_comentario"))
//      .orderBy(desc("fecha_comentario"))
//      .first().getAs[String]("nombre")
//    println(s"El usuario que ha realizado el comentario más reciente es: $usuario_ultimo_comentario")
//
//    //    Calcular el promedio de montos de ventas por mes:
//    val promedio_montos_ventas_por_mes = ventas_df
//      .groupBy(month(col("fecha_venta")).alias("mes"))
//      .agg(avg("monto_total").alias("promedio_montos_ventas"))
//    println("PROMEDIO MONTOS VENTAS POR MES")
//    promedio_montos_ventas_por_mes.show(false)
//
//    //    Calcular la suma total de montos de ventas por mes y por usuario:
//    val suma_montos_ventas_por_mes_y_usuario = ventas_df
//      .groupBy(month(col("fecha_venta")).alias("mes"), col("id_usuario"))
//      .agg(sum("monto_total").alias("suma_montos_ventas"))
//    println("SUMA MONTOS VENTAS POR MES Y USUARIO")
//    suma_montos_ventas_por_mes_y_usuario.show(false)
//
//    //    Encontrar el usuario con la venta de mayor monto:
//    val usuario_venta_mayor_monto = ventas_df
//      .join(usuarios_df, ventas_df("id_usuario") === usuarios_df("id_usuario"))
//      .groupBy(usuarios_df("id_usuario"), usuarios_df("nombre"))
//      .agg(max(ventas_df("monto_total")).alias("monto_total_venta"))
//      .orderBy(desc("monto_total_venta"))
//      .first().getAs[String]("nombre")
//    println(s"El usuario con la venta de mayor monto es: $usuario_venta_mayor_monto")
//
//    //    Calcular la cantidad de comentarios por usuario:
//    val cantidad_comentarios_por_usuario = comentarios_df
//      .groupBy("id_usuario")
//      .agg(count("*").alias("cantidad_comentarios"))
//    println("CANTIDAD COMENTARIOS POR USUARIOS")
//    cantidad_comentarios_por_usuario.show(false)
//
//    //    Obtener el mes con la mayor cantidad de comentarios:
//    val mes_mayor_cantidad_comentarios = comentarios_df
//      .groupBy(month(col("fecha_comentario")).alias("mes"))
//      .agg(count("*").alias("cantidad_comentarios"))
//      .orderBy(desc("cantidad_comentarios"))
//      .first().getAs[Int]("mes")
//    println(s"El mes con la mayor cantidad de comentarios es: $mes_mayor_cantidad_comentarios")
//
//    //    Calcular la diferencia de días entre la fecha de registro y la fecha de venta para cada usuario:
//    val diferencia_dias = usuarios_df
//      .join(ventas_df, usuarios_df("id_usuario") === ventas_df("id_usuario"))
//      .select(
//        usuarios_df("id_usuario"),
//        datediff(to_date(usuarios_df("fecha_registro")), to_date(ventas_df("fecha_venta"))).alias("diferencia_dias")
//      )
//    println("DIFERENCIA DIAS")
//    diferencia_dias.show(false)







    //    Calcular la cantidad total de ventas por producto:
//    val cantidad_ventas_por_producto = ventas_df
//      .groupBy("id_producto")
//      .agg(count("*").alias("cantidad_ventas"))
//    println("CANTIDAD VENTAS POR PRODUCTO")
//    cantidad_ventas_por_producto.show(false)

    //    Obtener el producto más vendido:
    //    val producto_mas_vendido = ventas_df
    //      .join(productos_df, ventas_df("id_producto") === productos_df("producto_id"))
    //      .groupBy(productos_df("producto_id"), productos_df("nombre"))
    //      .agg(sum(ventas_df("cantidad")).alias("total_ventas"))
    //      .orderBy(desc("total_ventas"))
    //      .first().getAs[String]("nombre")
    //    println(s"El producto más vendido es: $producto_mas_vendido")
    //
    //    //    Calcular la suma total de montos de ventas por categoría:
    //    val suma_montos_ventas_por_categoria = ventas_df
    //      .join(productos_df, ventas_df("id_producto") === productos_df("producto_id"))
    //      .groupBy(productos_df("categoria"))
    //      .agg(sum(ventas_df("monto_total")).alias("suma_montos_ventas"))
    //    println("SUMA MONTOS VENTAS POR CATEGORIA")
    //    suma_montos_ventas_por_categoria.show(false)
    //
    //    //    Encontrar la categoría con la mayor cantidad de ventas:
    //    val categoria_mayor_cantidad_ventas = ventas_df
    //      .join(productos_df, ventas_df("id_producto") === productos_df("producto_id"))
    //      .groupBy(productos_df("categoria"))
    //      .agg(count("*").alias("cantidad_ventas"))
    //      .orderBy(desc("cantidad_ventas"))
    //      .first().getAs[String]("categoria")
    //    println(s"La categoría con la mayor cantidad de ventas es: $categoria_mayor_cantidad_ventas")


  }
}
