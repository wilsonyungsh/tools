package com.wilson.spark.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object month {
  def main(args: Array[String]) {
    val (period) =
      args.size match {
        case x if x == 1 => (args(0))   // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYYMM")
          sys.exit(1)
        }
      }

    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._
    val input_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/nsw_base/"
    val trip = spark.read.parquet(input_path + period + "*").distinct
      .select("startTimeConverted", "agentId", "dominantMode", "origin_state", "origin_gcc", "origin_sa4", "destination_state", "destination_gcc", "destination_sa4", "distance", "weight")
      .withColumn("date", to_date('startTimeConverted))

    val output_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/"

    trip.write.parquet(output_path + "trip/" + period)
    //stop spark
    spark.stop
  }
}
