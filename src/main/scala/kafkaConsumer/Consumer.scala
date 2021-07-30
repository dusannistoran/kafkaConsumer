package kafkaConsumer

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

class Consumer(topic: String, brokers: String, groupId: String) {

  val consumer = new KafkaConsumer[String, String](configuration)

  private def configuration: Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props
  }

  def readMessages(): Unit = {

    consumer.subscribe(util.Arrays.asList(topic))

    while (true) {
      val records = consumer.poll(1000).asScala

      for (record <- records) {
        println(record.value())
      }
    }

  }

}

object Consumer {

  def main(args: Array[String]): Unit = {

    val consumer = new Consumer("test-topic", "localhost:9092", "test")
    consumer.readMessages()
  }

}
