package org.neu

import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import org.apache.spark.rdd.RDD

object Clustering {

  //Write to file
  def writeToFile(output: Array[String], outputPath: String): Unit = {
    val file = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(file))
    output.foreach(o => bw.write(o))
    bw.close()
  }

  //Topic wise count
  def topicCount(topic_cluster : RDD[(Int,String)],opPath: String): Unit = {
    val topic_count = topic_cluster.map(x => (x._1,1)).
      reduceByKey(_ + _).map(x => "Topic "+(x._1).toString+": "+x._2.toString+"\n").collect

    writeToFile(topic_count,opPath+"topic_distribution.txt")
  }

  //Topic wise count for high and low rating
  def Rating_topicCount(reviewsRDD : RDD[reviews],topic_reviews : RDD[(String,Int)],opPath: String): Unit = {
    val reviews_rating = reviewsRDD.map(rev => (rev.review_id,rev.stars)).persist

    val highRating_review = reviews_rating.filter(review => review._2 >= 4.0).persist
    val lowRating_review = reviews_rating.filter(review => review._2 < 4.0).persist

    val highRating_topic_count = highRating_review.join(topic_reviews).
      map{case(x,y) => (y._2,1)}.
      reduceByKey(_ + _).map(x => "Topic "+(x._1).toString+": "+x._2.toString+"\n").collect

    writeToFile(highRating_topic_count,opPath+"high_rating_topic_distribution.txt")

    val lowRating_topic_count = lowRating_review.join(topic_reviews).
      map{case(x,y) => (y._2,1)}.
      reduceByKey(_ + _).map(x => "Topic "+(x._1).toString+": "+x._2.toString+"\n").collect

    writeToFile(lowRating_topic_count,opPath+"low_rating_topic_distribution.txt")

  }


  def top10Businesses(reviewsRDD : RDD[reviews],topic_reviews : RDD[(String,Int)],
                      businessRDD : RDD[(String,(String,Float))],opPath: String): Unit = {


    val reviewsRDD_business = reviewsRDD.map(r => (r.review_id,r.business_id))
    var business_topic = topic_reviews.join(reviewsRDD_business).
      map{case(x,y) => (y._2,y._1)}.
      join(businessRDD).
      map{case(bId,(topic,(bname,brating))) => (topic,(bId,bname,brating))}.persist
    
  }


  def main(args: Array[String]): Unit = {

    // create Spark context with Spark configuration
    // val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))
    val sc = new SparkContext(new SparkConf().setAppName("Clustering"))
    sc.setLogLevel("ERROR")

    // read input file paths and output path
    val reviews = sc.textFile(args(0))
    val doc_topic = sc.textFile(args(1))
    val business = sc.textFile(args(2))
    val opPath = args(3)

    val reviewsRDD = reviews.mapPartitionsWithIndex {
      case (0, iter) => iter.drop(1)
      case (_, iter) => iter
    }.map(row => new reviews(row)).persist

    val businessRDD = business.mapPartitionsWithIndex {
      case (0, iter) => iter.drop(1)
      case (_, iter) => iter
    }.map(row => new business(row)).map(b => (b.business_id,(b.name,b.stars))).persist

    val topic_cluster = doc_topic.map(row => {
      val top_topic = new docTopic(row)
      ((top_topic.topic1,top_topic.review), (top_topic.topic2,top_topic.review))
    }).flatMap { x => Set(x._1, x._2) }.persist


    //Topic wise count
    topicCount(topic_cluster,opPath)

    //Topic wise count for high and low rating
    val topic_reviews = topic_cluster.map(_.swap)
    Rating_topicCount(reviewsRDD : RDD[reviews],topic_reviews,opPath)

    //Top businesses in each topic
    top10Businesses(reviewsRDD,topic_reviews, businessRDD ,opPath)

    println("Done!")

  }
}

