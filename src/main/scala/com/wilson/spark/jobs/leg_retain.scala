package com.wilson.spark.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object leg_retain {
  def main(args: Array[String]) {
    // check for args availability before getting paths
    val (period) =
      args.size match {
        case x if x == 1 => (args(0)) // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYYMM")
          sys.exit(1)
        }
      }
    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._
    val base_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/nsw_base/"
    val output_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/"
    val trip = spark.read.parquet(base_path + period + "*")
      .select("startTimeConverted", "agentId", "dominantMode", "origin_state", "origin_gcc", "origin_sa4",
        "destination_state", "destination_gcc", "destination_sa4", "linksInfo", "distance", "weight", "listOfModes", "listofLinks")
      .withColumn("retain", when(size('listOfModes) < 2, lit("1")).otherwise("0"))
      .drop("listOfModes", "linksInfo")
      .filter('retain === 1)
      .withColumn("date", to_date('startTimeConverted))

    //write out
    trip.write.parquet(output_path + "retained/" + period)
    //stop spark
    spark.stop
  }
}
