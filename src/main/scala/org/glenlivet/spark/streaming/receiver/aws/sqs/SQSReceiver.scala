package org.glenlivet.spark.streaming.receiver.aws.sqs

import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class SQSReceiver(name: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val timeout = 20000l

  override def onStart(): Unit = {
    val sqs = AmazonSQSClientBuilder.defaultClient();
    new Thread("SQS Polling") {

      def poll(): Unit = {
        val queueUrl = sqs.getQueueUrl(name).getQueueUrl
        val receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
        val messages = sqs.receiveMessage(receiveMessageRequest).getMessages
        for (message <- messages) {
          val payload = message.getBody
          store(payload)
          logger.debug(s"Received Message: ${payload}")
          sqs.deleteMessage(queueUrl, message.getReceiptHandle)
          logger.debug(s"Delete Message: ${payload}")
        }
        Thread.sleep(timeout)
        poll()
      }

      override def run() {
        try {
          poll()
        } catch {
          case e: IllegalArgumentException => restart(e.getMessage, e, 5000)
          case t: Throwable => restart("Connection error", t)
        }
      }
    }.start
  }

  override def onStop(): Unit = {
    //NOOP
  }
}
