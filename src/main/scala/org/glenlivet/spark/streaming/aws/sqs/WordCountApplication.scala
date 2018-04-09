package org.glenlivet.spark.streaming.aws.sqs

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.glenlivet.spark.streaming.receiver.aws.sqs.SQSReceiver
import org.slf4j.LoggerFactory

object WordCountApplication {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val name = "test-sqs"

    val sparkConf = new SparkConf().setAppName("CustomReceiver")
    sparkConf.setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val lines = ssc.receiverStream(new SQSReceiver(name))
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTerminationOrTimeout(120000l)
  }

}
