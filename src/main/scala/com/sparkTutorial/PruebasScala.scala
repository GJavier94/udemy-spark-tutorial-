package com.sparkTutorial

object PruebasScala {

  def funcionEscala1(a:Int,b: Int ): String ={
    (a + b).toString()
  }

  def main(args: Array[String]): Unit ={

    //pruebas funciones en scala
    val i = 2
    val j = 3
    val retorno = funcionEscala1(2, 3)
    println(retorno)

    // scala contenedor Option[T] es la foramde representar nulos en scala
    // el objeto puede ser Some[T] o none
    //un ejemplo lo podemos ver en el map

    val mapa = Map("Mexico"->"cdmx", "Francia"->"Paris")
    println(mapa.get("Mexico"))
    val capital = mapa.get("England")
    println(mapa.get("England"))

    if( (mapa.get("England")).isEmpty == true){
      println("no existe la llave")
    }

    val capitalString = show(capital)
    println(capitalString)


  }
  def show(x: Option[String]): String = {
    x match{
      case Some(s) => s
      case None => "null"
    }
  }
}

