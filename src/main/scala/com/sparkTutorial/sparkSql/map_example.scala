package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object map_example {

  def main(args: Array[String]): Unit = {
    val structureData = Seq(
      Row("36636","Finance",Row(3000,"USA")),
      Row("40288","Finance",Row(5000,"IND")),
      Row("42114","Sales",Row(3900,"USA")),
      Row("39192","Marketing",Row(2500,"CAN")),
      Row("34534","Sales",Row(6500,"USA"))
    )

    val structureSchema = new StructType()
      .add("id",StringType)
      .add("dept",StringType)
      .add("properties",new StructType()
        .add("salary",IntegerType)
        .add("location",StringType)
      )
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("HousePriceSolution").master("local[1]").getOrCreate()

    var df = session.createDataFrame(  session.sparkContext.parallelize(structureData), structureSchema)
    df.printSchema()
    df.show(false)

    val index = df.schema.fieldIndex("properties")
    val propSchema = df.schema(index).dataType.asInstanceOf[StructType]
  }

}
