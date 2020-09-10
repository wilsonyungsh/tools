package com.wilson.spark.jobs

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator //
import org.apache.spark.serializer.KryoSerializer //
import org.apache.spark.sql.SparkSession  //
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator //
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.sql.functions._
object leg_mapping_sa1 {
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
      .config("geospark.join.numpartition",50)
      .getOrCreate()

    GeoSparkSQLRegistrator.registerAll(spark)

    val wkt_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/nsw_sa1"
    val sa1 = spark.read.parquet(wkt_path).drop("origin_sa2","origin_sa3","origin_sa4")
    sa1.repartition(1024).createOrReplaceTempView("o_sa1")
    val destination_sa1 = spark.sql(
      """
        select origin_sa1 as destination_sa1,origin_gcc as destination_gcc, origin_state as destination_state,geom from o_sa1
        """.stripMargin
    )
    destination_sa1.repartition(1024).createOrReplaceTempView("d_sa1")
    val leg = spark.read.parquet("s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/month_geom/" + period).withColumn("uuid", expr("uuid()"))

    leg.repartition(1024).createOrReplaceTempView("leg_tomap")
    //sa1 join

    val mapped_o = spark.sql(
      """
        select a.uuid,b.origin_sa1,b.origin_gcc,b.origin_state from leg_tomap a,o_sa1 b where ST_WITHIN(a.s_geom,b.geom)
        """
    )

    val mapped_d = spark.sql(
      """
        select a.uuid,c.destination_sa1,c.destination_gcc,c.destination_state from leg_tomap a,d_sa1 c where ST_WITHIN(a.e_geom,c.geom)
        """
    )
    val full = leg.drop("s_geom","e_geom").join(mapped_o,Seq("uuid"),"left").join(mapped_d,Seq("uuid"),"left")


//write out
    val out_path = "s3a://au-daas-users/wilson/tfnsw/walkleg_trip/legs/monthly_leg_mapped/"
    full.write.mode("overwrite").parquet(out_path + period)
    //stop spark
    spark.stop
  }
}
