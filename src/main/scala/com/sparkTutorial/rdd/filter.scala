package com.sparkTutorial.rdd

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object filter {

  def main(args: Array[String]) {
    //se crea un logger para redirecciar las salida  a otro sitio
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]")
    //"spark://192.168.1.96:4040
    //val conf = new SparkConf().setAppName("wordCounts").setMaster("spark://192.168.1.96:4040")
    val sc = new SparkContext(conf)


    val lines = sc.textFile("in/uppercase.text")

    val linesFriday = lines.filter( line => line.contains("New York"))
    /*
    for ( line <- linesFriday) {
      println(line)
    }
    */
    //printRDD(linesFriday,"linesFriday" )

    val linesSinSpace = lines.filter(line => !line.isEmpty )

    printRDD(linesSinSpace,"linesSinSpace" )



  }

  def printRDD( rdd: RDD[String], name: String): Unit = {
    println(name)
    for ( line <- rdd) {
      println(line)
    }

  }

}
