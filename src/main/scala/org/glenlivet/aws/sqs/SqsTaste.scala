package org.glenlivet.aws.sqs

import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.sqs.model.CreateQueueRequest
import com.amazonaws.services.sqs.model.DeleteMessageRequest
import com.amazonaws.services.sqs.model.DeleteQueueRequest
import com.amazonaws.services.sqs.model.ReceiveMessageRequest
import com.amazonaws.services.sqs.model.SendMessageRequest
import scala.collection.JavaConversions._

object SqsTaste {

  def main(args: Array[String]): Unit = {

    val sqs = AmazonSQSClientBuilder.defaultClient

    System.out.println("===============================================")
    System.out.println("Getting Started with Amazon SQS Standard Queues")
    System.out.println("===============================================\n")

    try { // Create a queue
      System.out.println("Creating a new SQS queue called MyQueue.\n")
      val createQueueRequest = new CreateQueueRequest("MyQueue")
      val myQueueUrl = sqs.createQueue(createQueueRequest).getQueueUrl
      // List queues
      System.out.println("Listing all queues in your account.\n")
      for (queueUrl <- sqs.listQueues.getQueueUrls) {
        System.out.println("  QueueUrl: " + queueUrl)
      }
      System.out.println()
      // Send a message
      System.out.println("Sending a message to MyQueue.\n")
      sqs.sendMessage(new SendMessageRequest(myQueueUrl, "This is my message text."))
      // Receive messages
      System.out.println("Receiving messages from MyQueue.\n")
      val receiveMessageRequest = new ReceiveMessageRequest(myQueueUrl)
      val messages = sqs.receiveMessage(receiveMessageRequest).getMessages
      for (message <- messages) {
        System.out.println("Message")
        System.out.println("  MessageId:     " + message.getMessageId)
        System.out.println("  ReceiptHandle: " + message.getReceiptHandle)
        System.out.println("  MD5OfBody:     " + message.getMD5OfBody)
        System.out.println("  Body:          " + message.getBody)
        for (entry <- message.getAttributes.entrySet) {
          System.out.println("Attribute")
          System.out.println("  Name:  " + entry.getKey)
          System.out.println("  Value: " + entry.getValue)
        }
      }
      System.out.println()
      // Delete the message
      System.out.println("Deleting a message.\n")
      val messageReceiptHandle = messages.get(0).getReceiptHandle
      sqs.deleteMessage(new DeleteMessageRequest(myQueueUrl, messageReceiptHandle))
      // Delete the queue
      System.out.println("Deleting the test queue.\n")
      sqs.deleteQueue(new DeleteQueueRequest(myQueueUrl))
    } catch {
      case ase: AmazonServiceException =>
        System.out.println("Caught an AmazonServiceException, which means " + "your request made it to Amazon SQS, but was " + "rejected with an error response for some reason.")
        System.out.println("Error Message:    " + ase.getMessage)
        System.out.println("HTTP Status Code: " + ase.getStatusCode)
        System.out.println("AWS Error Code:   " + ase.getErrorCode)
        System.out.println("Error Type:       " + ase.getErrorType)
        System.out.println("Request ID:       " + ase.getRequestId)
      case ace: AmazonClientException =>
        System.out.println("Caught an AmazonClientException, which means " + "the client encountered a serious internal problem while " + "trying to communicate with Amazon SQS, such as not " + "being able to access the network.")
        System.out.println("Error Message: " + ace.getMessage)
    }
  }

}
