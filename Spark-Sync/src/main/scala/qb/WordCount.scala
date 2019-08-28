package qb


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.functions.broadcast

object SyncMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nqb.SyncMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val spark = SparkSession
            .builder()
            .appName("Follower Count")
            .getOrCreate()

      import spark.implicits._

  //  Delete output directory, only to ease local development; will not work on AWS. ===========
   val hadoopConf = new org.apache.hadoop.conf.Configuration
   val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
   try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
	// 	================

  // main code here

  val source = spark.read.csv(args(0) + "/source")
  val dest = spark.read.csv(args(0) + "/dest")

  val deleted = source.as("src").
      join(dest.as("dst"), ($"src._c0" === $"dst._c0"), "right")
      .select($"dst._c0")
      .filter($"src._c0".isNull)

  val inserted = source.as("src")
      .join(dest.as("dst"), $"src._c0" === $"dst._c0", "left_outer")
      .select($"src.*").filter($"dst._c0".isNull)

  val updated = dest.except(source)

  val tsvWithHeaderOptions: Map[String, String] = Map(
              ("delimiter", "\t"), // Uses "\t" delimiter instead of default ","
              ("header", "true"))  // Writes a header record with column names

    inserted
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("output/inserted")

    deleted
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("output/deleted")

    updated
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("output/updated")

  }
}