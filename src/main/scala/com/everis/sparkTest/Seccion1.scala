package com.everis.sparkTest

import java.util.Date
import java.text.SimpleDateFormat

object Seccion1{
  class Persona (nombre:String, edad:Int, dateNac:String){
    private val name =nombre
    private val age = edad
    private val fechaNac = dateNac
    def getName : String = name
    def getAge : Int = age
    def getDate: String = fechaNac
    def getYearNac : String = Utils.getYear(fechaNac)
  }

  class Coche (matricula:String, fechaMatr:String, modelo:String){
    private val matr =matricula
    def getMatrLetra= "A"
    private val fechaMatricula =fechaMatr
    def getFechaMatricula = fechaMatricula
    def getYearMatricula  = Utils.getYear(fechaMatricula)
    private val model = modelo
    def getModelo = model
  }

  class ContadorDeEjecucion {
    private val fechaInicio = new SimpleDateFormat("YYYYMMdd").format(new Date)
    private val horaInicio = new SimpleDateFormat("HH:mm:ss").format(new Date)
    def horaActual = new SimpleDateFormat("HH:mm:ss").format(new Date)
    def fechaActual = new SimpleDateFormat("YYYYMMdd").format(new Date)

    def getStartHour : String = horaInicio
    def getStartDate : String = fechaInicio
    def timeExec : String = TimeUtils.compareTimeHours(horaActual,horaInicio)
  }

}
