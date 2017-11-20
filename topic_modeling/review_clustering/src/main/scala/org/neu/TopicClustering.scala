package org.neu

import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import org.apache.spark.rdd.RDD

object TopicClustering {

  def writeToFile(output: String, outputPath: String): Unit = {
    val file = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(output)
    bw.close()
  }

  def main(args: Array[String]): Unit = {

    // create Spark context with Spark configuration
    // val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))
    val sc = new SparkContext(new SparkConf().setAppName("Review file generation for Topic Modeling"))
    sc.setLogLevel("ERROR")

    // read input file paths and output path
    val reviews = sc.textFile(args(0))


    val review_files = reviews.mapPartitionsWithIndex {
      case (0, iter) => iter.drop(1)
      case (_, iter) => iter
    }.map(row => {
      val review = new reviews(row)
      (review.review_id,review.review)
    }).foreach(r => writeToFile(r._2,"output/review_files/"+r._1))



//    val doc_topic = sc.textFile(args(0))
//    val opPath = args(1)
//
//    val topic_cluster = doc_topic.map(row => {
//      val top_topic = new docTopic(row)
//      ((top_topic.review,top_topic.topic1),(top_topic.review,top_topic.topic2))
//    }).flatMap{x => Set(x._1,x._2)}.persist
//
//    topic_cluster.foreach(println)

    println("Done!")

  }
}
