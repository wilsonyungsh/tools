package com.wilson.spark.jobs

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.aggregate.ApproximatePercentile
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.SparkSession

object staymetric_median_time {
  def main(args: Array[String]) {

    // check for args availability before getting paths
    val (month, state,output_path) =
      args.size match {
        case x if x == 3 => (args(0), args(1),args(2))   // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYYMM  (2) state code (3) output path")
          sys.exit(1)
        }
      }

    // create spark session
    val spark = SparkSession.builder()
      .getOrCreate()
    import spark.implicits._

    val INPUT_PATH = "s3a://au-daas-archive/wilson/StayMetric/preprocessed/"
    //val OUTPUT_PATH = "s3a://au-daas-users/wilson/tmr/staymetrics/"

    val pre_process = spark.read.parquet(INPUT_PATH + month + "*")
      .withColumn("date", regexp_replace(input_file_name(), "^.*?(\\d{8}).*$", "$1")) //get date
      .filter('distance_from_home_1000 && ((substring('home_sa2,1,1) === state) ||(substring('work_sa2,1,1) === state))) //filter for either home or work location in NSW
      .select("date","in_time_local","out_time_local","agent_id","home_sa2","work_sa2","stay_dur_mins","distance_from_home","w","coords")
      .withColumn("w", 'w.cast("Int")) //filter home category and qld only


    val geoh = spark.read.parquet("s3a://au-daas-compute/output/parquet/aggregated-geo-hierarchy/latest").selectExpr("sa2 as home_sa2","sa2_name","sa3_name","sa4_name","gcc_name").distinct
    import PercentileApprox._
    val daily_stay_home_sa2 = pre_process.withColumnRenamed("home_sa2", "target_sa2")
      .withColumn("target_sa2",'target_sa2.cast(StringType))
      .groupBy($"date",$"agent_id", $"target_sa2") //down to individual level
      .agg(
      max($"w").alias("w"),
      sum($"stay_dur_mins").alias("stay_dur_mins"))
      .withColumn("dup_id", explode(generateUdf(col("w")))) //duplicate agent and generate new agent id
      .withColumn("count", lit("1"))
      .withColumn("new_id",concat('agent_id,lit("_"),'dup_id))
      .orderBy("target_sa2","stay_dur_mins") //order by sa2 and duration
      .groupBy($"date",$"target_sa2")
      .agg(
        sum($"count").alias("unique_agents"),
        sum($"stay_dur_mins").alias("total_duration_mins"),
        mean($"stay_dur_mins").alias("mean_stay_duration"),  //calculate mean by exploded records
        percentile_approx($"stay_dur_mins",lit(0.5)).as("median_stay_duration")
      )
      .withColumn("avg_duration", $"total_duration_mins" / $"unique_agents") //calculate mean by division -should be the same as mean_stay_duration just for checking
      .orderBy("date","target_sa2")
      .withColumn("category",lit("1km within home_sa2"))
      .filter(substring('target_sa2,1,1) === state)
      .withColumnRenamed("target_sa2","home_sa2")
      .join(geoh,Seq("home_sa2"),"inner")

    //write out
    daily_stay_home_sa2.drop("avg_duration").coalesce(1).write.format("csv").mode(SaveMode.Overwrite)
      .option("sep", ",").option("header", "true")
      .save(output_path + month)

    //stop spark
    spark.stop
  }
  def generateUdf = udf((column: Int)=> (1 to column).toArray) //udf to duplicate rows
  //User Define Function for calculating quantiles
  object PercentileApprox {
    def percentile_approx(col: Column, percentage: Column, accuracy: Column): Column = {
      val expr = new ApproximatePercentile(
        col.expr,  percentage.expr, accuracy.expr
      ).toAggregateExpression
      new Column(expr)
    }
    def percentile_approx(col: Column, percentage: Column): Column = percentile_approx(
      col, percentage, lit(ApproximatePercentile.DEFAULT_PERCENTILE_ACCURACY)  //udf to calculate median
    )
  }

}
