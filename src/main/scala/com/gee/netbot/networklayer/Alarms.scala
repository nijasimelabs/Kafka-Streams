package com.gee.netbot.networklayer

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.connect.json.{JsonDeserializer, JsonSerializer};
import org.apache.kafka.streams.kstream.ForeachAction
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsBuilder}
import scala.collection.JavaConverters._

import com.gee.netbot.App
import com.gee.netbot.Constants._

object Alarms extends App {

  def getAppName(): String = {
    return ALARMS_APP_ID
  }


  def main(args: Array[String]) = {
    val stringSerde: Serde[String] = Serdes.String()
    val jsonSerializer: Serializer[JsonNode] = new JsonSerializer()
    val jsonDeserializer: Deserializer[JsonNode] = new JsonDeserializer()
    val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)

    val streamConfig = getAppProperties() match {
      case Some(props) => props;
      case None => {
        logger.error("Could not read application properties")
        throw new Exception("Could not read application properties")
      }
    }

    val prodConfig = getProducerProperties() match {
      case Some(props) => props;
      case None => {
        logger.error("Could not read producer properties")
        throw new Exception("Could not read producer properties")
      }
    }

    val producer = new KafkaProducer[String, String](prodConfig)

    val builder: StreamsBuilder = new StreamsBuilder()

    logger.debug("Listening on topic: {}", getConfig(TOPIC_ALARMS))

    val alarmsStream = builder.stream(
      getConfig(TOPIC_ALARMS),
      Consumed.`with`(stringSerde, jsonSerde)
    ).foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {
          val severity = value.get("severity").asText
          val outtopic = if (severity == "Critical") {
            getConfig(ALARMS_SEVERE)
          } else {
            getConfig(ALARMS_NONSEVERE)
          }
          logger.debug("sending alarm to {} : severity: {}", outtopic, severity)
          val result = new ProducerRecord[String, String](outtopic, key, value.toString())
          producer.send(result)
        }
      }
    )

    val streamApp : KafkaStreams = new KafkaStreams(builder.build(), streamConfig)
    streamApp.start();
  }
}
