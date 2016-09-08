package com.bakery.woople.kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}


class DataProducer(brokers: String, topic: String) extends Runnable {

  val brokerList = brokers
  val props = new Properties()
  props.put("metadata.broker.list", this.brokerList)
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("producer.type", "async")
  props.put("partitioner.class", "com.bakery.woople.kafka.SimplePartitioner")
  val config = new ProducerConfig(this.props)
  val producer = new Producer[String, String](this.config)

  val PHONE_MAX = 100


  def run(): Unit = {
    for (key <- 1 to 10 ) {

      sendMessage(key.toString, "2015-11-11T15:00:00|~|xxx|~|125.119.144." + key + "|~|xxx|~|xxx|~|xxx|~|C0EA13670E0B942E70E")
    }

  }

  def sendMessage(key: String, message: String) = {
    try {
      val data = new KeyedMessage[String, String](this.topic, key, message);
      producer.send(data);
    } catch {
      case e: Exception => println(e)
    }
  }
}

object DataProducerClient {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage:DataProducer <metadata.broker.list> <topic>")
      System.exit(1)
    }
    //start the message producer thread
    new Thread(new DataProducer(args(0), args(1))).start()
  }
}
