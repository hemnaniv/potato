package qb


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

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
  val tsvWithHeaderOptions: Map[String, String] = Map(
  ("delimiter", "\t"), // Uses "\t" delimiter instead of default ","
  ("header", "true"))  // Writes a header record with column names

  // main code here

  val source = spark.read.format("json").json("../testSparkSyncDataSetOriginal.json")


    // val textFile = sc.textFile(args(0))
    // val counts = textFile.flatMap(line => line.split(" "))
    //              .map(word => (word, 1))
    //              .reduceByKey(_ + _)
    // source.saveAsTextFile(args(1))
    //  counts.saveAsTextFile(args(1))
    // logger.info(counts.toDebugString)
    
  }
}