package com.wilson.spark.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object legExplosion {

  def main(args: Array[String]) {
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


    val base_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/nsw_base/"

    val trip = spark.read.parquet(base_path + day)
      .select("startTimeConverted", "agentId", "dominantMode", "origin_state","origin_gcc", "origin_sa4",
        "destination_state", "destination_gcc", "destination_sa4","linksInfo","distance", "weight","listOfModes","listofLinks")
      .withColumn("retain",when(size('listOfModes) < 2,lit("1")).otherwise("0"))
      .drop("listOfModes")
      .filter('retain === 0)

    val leg = trip.withColumn("leg_info",explode('linksInfo)).drop("linksInfo")
      .withColumn("leg_route",'leg_info.getItem("routeTaken"))
      .withColumn("leg_start_time",split('leg_route,"/").getItem(2))
      .withColumn("leg_endtime_split",split('leg_route,"/"))
      .withColumn("leg_size",size('leg_endtime_split))
      .withColumn("leg_end_time",'leg_endtime_split('leg_size -3))
      .drop("leg_endtime_split","leg_size")
      .withColumn("leg_mode",'leg_info.getItem("mode"))
      .withColumn("start_loc",concat('leg_info.getItem("startLon"),lit(","),'leg_info.getItem("startLat")))
      .withColumn("end_loc",concat('leg_info.getItem("endLon"),lit(","),'leg_info.getItem("endLat")))
      .withColumn("leg_duration",unix_timestamp($"leg_end_time") - unix_timestamp($"leg_start_time")) //calculate leg duration
      .withColumn("leg_start_date",substring('leg_start_time,1,10)) //calcuate leg start date
      .drop("leg_info","listOfModes","listOfLinks")
      .withColumn("s_leg_route",explode(split('leg_route,"\\->"))) //explode trip leg to subleg in order to calculate leg distance
      .withColumn("s_links",split('s_leg_route,"\\/").getItem(1)) //subleg links
      .withColumn("s_link_direction",split('s_leg_route,"/").getItem(0)) //subleg links
      .withColumn("s_dis",split('s_leg_route,"\\/").getItem(4))
      .orderBy("agentId","startTimeConverted","leg_start_time")
      .groupBy("agentId","startTimeConverted","dominantMode","origin_state","destination_state","origin_gcc","destination_gcc","origin_sa4","destination_sa4","leg_start_date","leg_mode","leg_start_time","leg_end_time","leg_duration","leg_route","start_loc","end_loc")
      .agg(sum("s_dis").alias("leg_distance"),
        collect_list('s_links).alias("leg_links"),
        max("weight").alias("weight"))
      .drop("leg_route","origin_state","destination_state","origin_gcc","destination_gcc")


    //write out
    val output_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/"
    leg.write.parquet(output_path + "exploded_not_mapped/" + day)

    //stop spark
    spark.stop
  }
}