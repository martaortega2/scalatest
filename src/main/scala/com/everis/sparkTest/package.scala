package com.everis

package object sparkTest {
  object Utils {
    def getDay(fecha:String):String={
      fecha.split("/")(0)
    }
    def getMonth(fecha:String):String={
      fecha.split("/")(1)
    }
    def getYear(fecha:String):String={
      fecha.split("/")(2)
    }
  }

  object TimeUtils {
    def compareSecs(time1:String,time2:String):(Int,Int)={
      val time1M = time1.split(":")(1).toInt
      val time2M = time2.split(":")(1).toInt
      val time1S = time1.split(":")(2).toInt
      val time2S = time2.split(":")(2).toInt
      if (time1S < time2S){
        (60 + time1S - time2S,time2M+1)
      } else {
        (time1S - time2S, time2M)
      }
    }

    def compareMins(time1:String,time2:String):(Int,Int)={
      val time1H = time1.split(":")(0).toInt
      val time2H = time2.split(":")(0).toInt
      val time1M = time1.split(":")(1).toInt
      val time2M = time2.split(":")(1).toInt
      if (time1M < time2M){
        (60 + time1M - time2M,if(time2H+1==24){0}else{time2H+1})
      } else {
        (time1M - time2M, time2H)
      }
    }

    def compareHours (time1H:Int,time2H:Int):Int={
      if (time1H < time2H){
        24+time1H-time2H
      } else {
        time1H-time2H
      }
    }

    // time1 - time2
    def compareTimeHours(time1:String,time2:String):String={
      val time1S=time1.split(":")(2)
      val time2S=time2.split(":")(2)
      val time1H=time1.split(":")(0)
      val (difSecs,time2M) = compareSecs(time1,time2)
      val (difMin,time2H) = compareMins(time1,s"${time2.split(":")(0)}:$time2M:${time2.split(":")(2)}")
      val difHours = compareHours(time1H.toInt,time2H)
      s"$difHours:$difMin:$difSecs"
    }
  }
}
