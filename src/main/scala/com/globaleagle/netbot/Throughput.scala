package com.globaleagle.netbot

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode};
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.connect.json.{JsonDeserializer, JsonSerializer};
import org.apache.kafka.streams.kstream.{Aggregator, Initializer, Materialized,
  TimeWindows, ValueMapper, Windowed }
import org.apache.kafka.streams.state.WindowStore
import org.apache.kafka.streams.{Consumed, KafkaStreams, StreamsBuilder}
import scala.collection.JavaConverters._

import Constants._

object Throughput extends App {

  def getAppName(): String = {
    return THROUGHPUT_APP_ID
  }

  def main(args: Array[String]): Unit = {

    val stringSerde: Serde[String] = Serdes.String()
    val jsonSerializer: Serializer[JsonNode] = new JsonSerializer()
    val jsonDeserializer: Deserializer[JsonNode] = new JsonDeserializer()
    val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)

    val streamConfig = getAppProperties() match {
      case Some(props) => props;
      case None => {
        throw new Exception("Could not read application properties")
      }
    }

    val prodConfig = getProducerProperties() match {
      case Some(props) => props;
      case None => {
        throw new Exception("Could not read producer properties")
      }
    }

    val producer = new KafkaProducer[String, String](prodConfig)

    val builder: StreamsBuilder = new StreamsBuilder()
    val store: ObjectNode = JsonNodeFactory.instance.objectNode();

    val windowSizeMs = 10 * 1000 // 10 sec

    //define stream here
    val throughputStream = builder.stream(
      TOPIC_THROUGHPUT,
      Consumed.`with`(stringSerde, jsonSerde)
    ).groupByKey().windowedBy(
      TimeWindows.of(windowSizeMs)
    ).aggregate(
      new Initializer[JsonNode]() { /* initializer */
        override def apply(): JsonNode = {
          val initVal: ObjectNode = JsonNodeFactory.instance.objectNode();
          initVal.set("min", JsonNodeFactory.instance.numberNode(-1));
          initVal.set("max", JsonNodeFactory.instance.numberNode(-1));
          return initVal;
        }
      },
      new Aggregator[String, JsonNode, JsonNode]() { /* aggregator */
        def apply(aggKey: String, newValue: JsonNode, aggValue: JsonNode): JsonNode = {
          val throughput = newValue.get("bytes").asDouble
          var currentMin = aggValue.get("min").asDouble()
          var currentMax = aggValue.get("max").asDouble()

          if (currentMin == -1 || throughput < currentMin) {
            currentMin = throughput
          }

          if (currentMax == -1 || throughput > currentMax) {
            currentMax = throughput
          }

          aggValue.asInstanceOf[ObjectNode].set("min", JsonNodeFactory.instance.numberNode(currentMin))
          aggValue.asInstanceOf[ObjectNode].set("max", JsonNodeFactory.instance.numberNode(currentMax))

          // set classname, linkname and direction
          aggValue.asInstanceOf[ObjectNode].set("trafficClass", newValue.get("trafficClass"))
          aggValue.asInstanceOf[ObjectNode].set("link", newValue.get("link"))
          aggValue.asInstanceOf[ObjectNode].set("direction", newValue.get("direction"))
          return aggValue;
        }
      },
      Materialized.as[String, JsonNode, WindowStore[Bytes, Array[Byte]]]("troughput-store").withValueSerde(jsonSerde).withKeySerde(stringSerde)
    ).mapValues(
      new ValueMapper[JsonNode, JsonNode]() {
        def apply(finalVal: JsonNode): JsonNode = {
          val minBytes = finalVal.get("min").asDouble
          val maxBytes = finalVal.get("max").asDouble
          val throughputpbs = (maxBytes - minBytes) * 8 / 10

          finalVal.asInstanceOf[ObjectNode].without(List("min", "max").asJava).asInstanceOf[ObjectNode].set(
            "throughput_bps",
            JsonNodeFactory.instance.numberNode(throughputpbs)
          )

          // send results to result topic
          val key = finalVal.get("link").asText() + "-" + finalVal.get("trafficClass").asText() + "-" + finalVal.get("direction").asText()
          val result = new ProducerRecord[String, String](THROUGHPUT_RESULT_TOPIC, key, finalVal.toString())
          producer.send(result)
          return finalVal
        }
      }
    )

    val streamApp : KafkaStreams = new KafkaStreams(builder.build(), streamConfig)
    streamApp.start();
  }

}
