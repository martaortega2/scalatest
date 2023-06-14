package com.everis.sparkTest

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class TestSeccion3Spark extends FlatSpecLike with TestBase with Matchers {
  "modelizacion" should "work" in {
    val ss = sparkSql
    import ss.implicits._
    val estrucPers = List(("ID1", "Javi",24, "1996-10-31"),("ID2","Alberto", 56, "1955-02-02"),("ID3","Luisa",33,"1988-01-01")).toDF("ID","Nombre","Edad","FecNacim")
    val centros = List(("0157", "Josefa Valcarcel"),("2223","La Vela")).toDF("idcent","nombre")
    val empleados = List(("ID1", "Santander Tecnologia","2017-10-10", "0157"),("ID2","BBVA", "2020-12-12","0157"),("ID3","BBVA","2015-03-02","2223")).toDF("ID","Cliente","HireDate","Office")

    val salida1 = List(("BBVA","0157","*******","2020-12-12"),("BBVA","2223","*****","2015-03-02")).toDF("Cliente","Centro", "NombreEmpleado", "HireDate")
    val salida2 = List(("Santander Tecnologia","0157","****","2017-10-10")).toDF("Cliente","Centro", "NombreEmpleado", "HireDate")

    (Seccion3.modelizacion(empleados,centros,estrucPers)._1.except(salida1).count, Seccion3.modelizacion(empleados,centros,estrucPers)._2 except salida2 count) shouldBe (0,0)

  }
}
