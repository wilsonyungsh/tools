package com.wilson.spark.jobs

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator //
import org.apache.spark.serializer.KryoSerializer //
import org.apache.spark.sql.SparkSession  //
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator //
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.functions._

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

    val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("geospark.global.index", "true")   //index
      .config("geospark.global.indextype", "quadtree")
      .config("geospark.join.numpartition",1024)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)

    //import spark.implicits._

    GeoSparkSQLRegistrator.registerAll(spark) //register utils

    //using sa4
    val wkt_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/nsw_sa4"
    val sa4 = spark.read.parquet(wkt_path)
    sa4.createOrReplaceTempView("o_sa4")
    val destination_sa4 = spark.sql(
      """
        select origin_sa4 as destination_sa4,origin_gcc as destination_gcc, origin_state as destination_state,geom from o_sa4
        """.stripMargin
    )
    destination_sa4.createOrReplaceTempView("d_sa4")

    val input_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/month_geom/"
    val leg = spark.read.parquet(input_path + period).withColumn("uuid", expr("uuid()")) //create uuid as key to join later on
    leg.createOrReplaceTempView("leg_tomap")
    // spatial join to bring in origin
    val mapped_o = spark.sql(
      """
        select a.uuid,b.origin_sa4,b.origin_gcc,b.origin_state from leg_tomap a,o_sa4 b where ST_WITHIN(a.s_geom,b.geom)
        """
    )
    // spatial join to bring in destination
    val mapped_d = spark.sql(
      """
        select a.uuid,c.destination_sa4,c.destination_gcc,c.destination_state from leg_tomap a,d_sa4 c where ST_WITHIN(a.e_geom,c.geom)
        """
    )
    // join to master table based on uuid
    val full = leg.join(mapped_o,Seq("uuid"),"left").join(mapped_d,Seq("uuid"),"left")
    //write out
    val out_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/monthly_leg_mapped/"
    full.write.mode("overwrite").parquet(out_path + period)
    //stop spark
    spark.stop
  }
}
