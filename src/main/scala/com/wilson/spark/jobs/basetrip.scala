package com.wilson.spark.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object basetrip {

  def main(args: Array[String]) {
    // check for args availability before getting paths
    val (day) =
      args.size match {
        case x if x == 1 => (args(0))   // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYYMMDD")
          sys.exit(1)
        }
      }
    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._
    val output_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/"
    val weight_dir = "s3a://au-daas-compute/xtrapolation_for_roamer/merge_imsi_weight/"


    val geo_o = spark.read.parquet("s3a://au-daas-compute/output/parquet/aggregated-geo-hierarchy/latest")
                .selectExpr("geo_hierarchy_base_id as startGeoUnitId","state as origin_state","gcc as origin_gcc","sa4 as origin_sa4","sa3 as origin_sa3")
    val geo_d = geo_o.selectExpr("startGeoUnitId as endGeoUnitId","origin_state as destination_state","origin_gcc as destination_gcc","origin_sa4 as destination_sa4","origin_sa3 as destination_sa3")
    //val trip_path = "s3a://au-daas-compute/output/parquet/union_trip/"
    val trip_path = "s3a://au-daas-compute/output-v2/parquet-v2/union_trip/"

    val weights = spark.read.csv(weight_dir + day)
      .withColumn("file_date", regexp_replace(input_file_name(), "^.*?(\\d{8}).*$", "$1"))
      .selectExpr("_c0 as agentId", "_c1 as weight","file_date")
      .withColumn("weight",'weight.cast("Int"))
      .filter('weight =!= 0)

    val trip = spark.read.parquet(trip_path + day)
      .withColumn("file_date", regexp_replace(input_file_name(), "^.*?(\\d{8}).*$", "$1"))
      .join(weights,Seq("agentId","file_date"),"inner")
      .select("startGeoUnitId","endGeoUnitId","startTime","endTime","agentId","distance","listOfModes","listOfLinks","linksInfo","dominantMode","weight")
      .join(geo_o,Seq("startGeoUnitId"),"left")
      .join(geo_d,Seq("endGeoUnitId"),"left")
      .filter('origin_state === 1 || 'destination_state === 1)
      .withColumn("startTimeConverted",adjustTimeByStateId('startTime,'origin_state))

    trip.write.mode(SaveMode.Overwrite).parquet(output_path + "nsw_base/" + day)
  }

  def adjustTimeByStateId(time: Column, state: Column): Column = {
    when(state === "1", from_utc_timestamp(time, "Australia/Sydney"))
      .when(state === "2", from_utc_timestamp(time, "Australia/Melbourne"))
      .when(state === "3", from_utc_timestamp(time, "Australia/Brisbane"))
      .when(state === "4", from_utc_timestamp(time, "Australia/Adelaide"))
      .when(state === "5", from_utc_timestamp(time, "Australia/Perth"))
      .when(state === "6", from_utc_timestamp(time, "Australia/Hobart"))
      .when(state === "7", from_utc_timestamp(time, "Australia/Darwin"))
      .when(state === "8", from_utc_timestamp(time, "Australia/Sydney"))
      .otherwise(from_utc_timestamp(time, "Australia/Sydney")) // default to Sydney timezone
  }
}
