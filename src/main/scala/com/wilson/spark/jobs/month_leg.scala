package com.wilson.spark.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object month_leg {
  def main(args: Array[String]) {
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
    val input_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/exploded_not_mapped/"
    val leg = spark.read.parquet(input_path + period + "*").distinct.drop("origin_sa4","destination_sa4","dominantMode","startTimeConverted")
      .withColumn("start_lon",split('start_loc,",")(0).cast("Double"))
      .withColumn("start_lat",split('start_loc,",")(1).cast("Double"))
      .withColumn("end_lon",split('end_loc,",")(0).cast("Double"))
      .withColumn("end_lat",split($"end_loc",",")(1).cast("Double"))
      .withColumn("listOfLinks",concat_ws(",",'leg_links)).drop("leg_links")
      .drop("start_loc","end_loc")

    //write out
    val out_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/exploded_month_not_mapped/"
    leg.write.parquet(out_path + period)
// stop
    spark.stop
  }

}
