package com.everis.sparkTest

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}

@RunWith(classOf[JUnitRunner])
class TestSeccion2FlatSpec extends FlatSpecLike with Matchers{

  "traduccion" should "work" in {
    val texto = "Un dia vi una vaca vestida de uniforme"
    Seccion2.traduccion(texto) shouldBe 333
  }
}
