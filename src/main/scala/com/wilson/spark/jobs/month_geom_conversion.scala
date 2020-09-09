package com.wilson.spark.jobs

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

object month_geom_conversion {

  def main(args: Array[String]) {
    val (period) =
      args.size match {
        case x if x == 1 => (args(0)) // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYYMMDD")
          sys.exit(1)
        }
      }

    val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("geospark.global.index", "true") //index
      .config("geospark.global.indextype", "quadtree")
      .config("geospark.join.gridtype", "quadtree")
      .config("geospark.join.numpartition", 1024)
      .getOrCreate()

    //import spark.implicits._
    GeoSparkSQLRegistrator.registerAll(spark) //register utils

    val path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/exploded_month_not_mapped/"
    val leg = spark.read.parquet(path + period)
    leg.createOrReplaceTempView("leg_a")

    val leg_geom = spark.sql(
      """
        select agentId agentId,leg_mode,leg_start_date,leg_start_time,leg_end_time,leg_duration,leg_distance,listOflinks,weight,
               st_point(CAST(start_lon as Decimal(24,20)),CAST(start_lat as Decimal(24,20))) as s_geom,
               st_point(CAST(end_lon as Decimal(24,20)),CAST(end_lat as Decimal(24,20))) as e_geom from leg_a
        """.stripMargin).repartition(1024)

    val output_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/"
    leg_geom.write.parquet(output_path + "month_geom/" + period)
    // stop spark
    spark.stop
  }
}
