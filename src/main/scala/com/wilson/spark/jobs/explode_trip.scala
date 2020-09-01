package com.wilson.spark.jobs



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.functions._

object explode_trip {
  def main(args: Array[String]) {
    // check for args availability before getting paths
    val (day) =
      args.size match {
        case x if x == 1 => (args(0)) // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYYMMDD")
          sys.exit(1)
        }
      }
    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._
    val output_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/"
    val trip_path = output_path + "nsw_base/"

    val trip = spark.read.parquet(trip_path + day)
      .withColumn("leg_info", explode('linksInfo)).drop("linksInfo") //explode trip down to leg level
      .withColumn("leg_route", 'leg_info.getItem("routeTaken"))
      .withColumn("leg_start_time", split('leg_route, "/").getItem(2))
      .withColumn("leg_start_time", to_timestamp('leg_start_time))
      .withColumn("leg_endtime_split", split('leg_route, "/"))
      .withColumn("leg_size", size('leg_endtime_split))
      .withColumn("leg_end_time", 'leg_endtime_split ('leg_size - 3))
      .withColumn("leg_end_time", to_timestamp('leg_end_time))
      .withColumn("leg_mode", 'leg_info.getItem("mode"))
      .withColumn("s_leg_route", explode(split('leg_route, "\\->"))) //explode trip leg to subleg in order to calculate leg distance
      .withColumn("s_links", split('s_leg_route, "\\/").getItem(1)) //subleg links
      .withColumn("s_link_direction", split('s_leg_route, "/").getItem(0)) //subleg links
      .withColumn("s_dis", split('s_leg_route, "\\/").getItem(4))
      .orderBy("agentId", "startTimeConverted", "leg_start_time")
      .groupBy("agentId", "startTimeConverted", "dominantMode", "origin_state", "destination_state", "origin_gcc", "destination_gcc", "origin_sa4", "destination_sa4", "origin_sa3", "destination_sa3", "listOfModes", "leg_mode", "leg_start_time", "leg_end_time", "listOfLinks", "leg_route", "distance")
      .agg(sum("s_dis").alias("leg_distance"),
        collect_list('s_links).alias("leg_links"),
        max("weight").alias("weight"))
      .withColumnRenamed("distance", "original_trip_distance")
      .drop("listOfLinks", "leg_route")
      .withColumn("date", to_date($"startTimeConverted"))

    trip.write.mode(SaveMode.Overwrite).parquet(output_path + "exploded_trip/" + day)

    //stop spark
    spark.stop
  }
}

