package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object WordCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.WordCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

	//  Delete output directory, only to ease local development; will not work on AWS. ===========
  //  val hadoopConf = new org.apache.hadoop.conf.Configuration
  //  val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
  //  try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
	// 	================
    
    val textFile = sc.textFile(args(0))
    val counts = textFile.map(line => line.split(","))
                      .flatMap(word => List((word(0), 0), (word(1), 1))) //mapping unfollowed users too : bonus challenge 2
                      .reduceByKey(_ + _)
                      .map(word => (word._2, word._1)) //swapping key and value for sorting : bonus challenge 1
                      .sortByKey(false)
                      .map(word => (word._2, word._1)) //swapping key and value for final output

    logger.info(counts.toDebugString)
    counts.saveAsTextFile(args(1))
  }
}