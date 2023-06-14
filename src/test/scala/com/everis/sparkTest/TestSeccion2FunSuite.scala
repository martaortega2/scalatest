package com.everis.sparkTest

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestSeccion2FunSuite extends FunSuite{

  test("traduccion should work")  {
    val texto = "Un dia vi una vaca vestida de uniforme"
    assert(Seccion2.traduccion(texto) ==333)
  }

  test("test simple")  {
    val texto = "a"
    assert(Seccion2.traduccion(texto) ==1)
  }
}
