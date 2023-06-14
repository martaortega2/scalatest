package com.everis.sparkTest

object Seccion2 {
  object ApplyMap {
    val traduccionSt =  Map("A"->1 , "B"->2 , "C"->3 , "D"->4 , "E"->5 , "F"->6 , "G"->7 , "H"->8 , "I"->9 , "J"->10,
      "K"->11, "L"->12, "M"->13, "N"->14, "O"->15, "P"->16, "Q"->17, "R"->18, "S"->19, "T"->20, "U"->21, "V"->22,
      "W"->23, "X"->24, "Y"->25, "Z"->26)
    val traduccionCh =  Map('A'->1 , 'B'->2 , 'C'->3 , 'D'->4 , 'E'->5 , 'F'->6 , 'G'->7 , 'H'->8 , 'I'->9 , 'J'->10,
      'K'->11, 'L'->12, 'M'->13, 'N'->14, 'O'->15, 'P'->16, 'Q'->17, 'R'->18, 'S'->19, 'T'->20, 'U'->21, 'V'->22,
      'W'->23, 'X'->24, 'Y'->25, 'Z'->26)

    def applyMap(text:String):Int={
      traduccionSt.getOrElse(text.toUpperCase,0)
    }
    def applyMap(texto:Character):Int={
      traduccionCh.getOrElse(Character.toUpperCase(texto),0)
    }
  }
  // Aplicas la funcion anterior al texto dado
  def traduccion (textInput: String)= {
    textInput.foldLeft(List.empty[Int])((acc, x) => acc ::: List(ApplyMap.applyMap(x.toString))).sum
    }
  }
