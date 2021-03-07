package com.sparkTutorial.sparkSql.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{SparkSession, functions}

object UkMakerSpaces {
  val POSTAL_CODE = "Postcode"
  val REGION = "Region"
  val MAKERSPACESPERREGION = "maker_spaces_per_region"
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val session = SparkSession.builder().appName("UkMakerSpaces").master("local[*]").getOrCreate()

    val makerSpace = session.read.option("header", "true")
      .csv("in/uk-makerspaces-identifiable-data.csv")

    makerSpace.show()
    val postalCode = session.read.option("header", "true")
      .csv("in/uk-postcode.csv").withColumn(  POSTAL_CODE, functions.concat( functions.col(POSTAL_CODE), functions.lit(" ") ))
    postalCode.select(POSTAL_CODE).show(2)



    val postal_code_maker_space_joined = makerSpace.join(postalCode, makerSpace.col(POSTAL_CODE).startsWith( postalCode.col(POSTAL_CODE)), "left_outer")
    postal_code_maker_space_joined.show(5)

    print( postal_code_maker_space_joined.count() )

    val makerSpacesGroupedByRegion = postal_code_maker_space_joined.groupBy(
      postal_code_maker_space_joined.col(REGION)
    ).agg(count(REGION).alias(MAKERSPACESPERREGION))
      .orderBy(functions.col(MAKERSPACESPERREGION).desc)

    makerSpacesGroupedByRegion.show(10)
    /*
      withColumnRenamed("count")
    .orderBy(postal_code_maker_space_joined.col("count"))
    makerSpacesGroupedByRegion.show(10)



    val makerSpace = session.read.option("header", "true").csv("in/uk-makerspaces-identifiable-data.csv")


    val postCode1 = session.read.option("header", "true").csv("in/uk-postcode.csv")


    postCode1.select("PostCode").show()

    val postCode2 = session.read.option("header", "true").csv("in/uk-postcode.csv")
       .withColumn("PostCode", functions.concat_ws("", functions.col("PostCode"), functions.lit(" ")))

    postCode2.select("PostCode").show()


    System.out.println("=== Print 20 records of makerspace table ===")
    makerSpace.select("Name of makerspace", "Postcode").show()

    System.out.println("=== Print 20 records of postcode table ===")
    postCode.show()

    val joined = makerSpace.join(postCode, makerSpace.col("Postcode").startsWith(postCode.col("Postcode")), "left_outer")

    System.out.println("=== Group by Region ===")
    joined.groupBy("Region").count().show(200)

     */
  }
}
