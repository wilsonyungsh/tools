package com.wilson.spark.jobs



import org.apache.spark.sql.SparkSession  //
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator //
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.serializer.KryoSerializer //
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator //

object leg_mapping {
  def main(args: Array[String]) {
    val (period) =
      args.size match {
        case x if x == 1 => (args(0)) // check input parameters
        case _ => {
          sys.error("Invalid arguments. Please pass (1) YYYYMMDD")
          sys.exit(1)
        }
      }

    val spark = SparkSession.builder().config("geospark.join.gridtype", "kdbtree")
      //.master("local")
      .getOrCreate()

    //import spark.implicits._

    GeoSparkSQLRegistrator.registerAll(spark) //register utils

    val wkt_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/nsw_sa4"
    val sa4 = spark.read.parquet(wkt_path)
    sa4.createOrReplaceTempView("sa4_wkt")

    val origin_sa4 = spark.sql(
      """
        select SA4_CODE16 as origin_sa4,GCC_CODE16 as origin_gcc, STE_CODE16 as origin_state,ST_GeomFromWKT(geometry) as geom from sa4_wkt
        """.stripMargin
    )

    origin_sa4.createOrReplaceTempView("o_sa4")

    val destination_sa4 = spark.sql(
      """
        select SA4_CODE16 as destination_sa4,GCC_CODE16 as destination_gcc, STE_CODE16 as destination_state,ST_GeomFromWKT(geometry) as geom from sa4_wkt
        """.stripMargin
    )

    destination_sa4.createOrReplaceTempView("d_sa4")


    val input_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/exploded_month_not_mapped/"
    val leg = spark.read.parquet(input_path + period)
    leg.createOrReplaceTempView("leg_a")

    //convert into geometry
    val leg_geom = spark.sql(
      """
        select agentId agentId,leg_start_date,leg_mode,leg_start_time,leg_end_time,leg_duration,leg_distance,leg_links,weight,
               st_point(CAST(start_lon as Decimal(24,20)),CAST(start_lat as Decimal(24,20))) as s_geom,
               st_point(CAST(end_lon as Decimal(24,20)),CAST(end_lat as Decimal(24,20))) as e_geom from leg_a
        """
    )
    leg_geom.createOrReplaceTempView("leg_tomap")

    //map out start and end geography
    val mapped = spark.sql(
      """
        select a.*,b.origin_sa4,b.origin_gcc,b.origin_state,c.destination_sa4,c.destination_gcc,c.destination_state from leg_tomap a
        |left join o_sa4 b on ST_WITHIN(a.s_geom,b.geom)
        |left join d_sa4 c on ST_WITHIN(a.e_geom,c.geom)
        """
    ).drop("s_geom", "e_geom")

    //write out
    val out_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/"
    mapped.write.mode(SaveMode.Overwrite).parquet(out_path + "monthly_leg_mapped/" + period)
    //stop spark
    spark.stop
  }
}
