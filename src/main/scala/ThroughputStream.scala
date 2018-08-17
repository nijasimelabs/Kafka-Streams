import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode, ArrayNode, TextNode};
import java.util.Properties
import java.util.function.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.serialization._
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{ValueMapper, KStream, KTable, Produced, Serialized, TimeWindows, Initializer, Aggregator}
import scala.collection.JavaConverters._

import Constants._

object ThroughputStream {

  def main(args: Array[String]): Unit = {
    val stringSerde: Serde[String] = Serdes.String()
    val jsonSerializer: Serializer[JsonNode] = new JsonSerializer()
    val jsonDeserializer: Deserializer[JsonNode] = new JsonDeserializer()
    val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)


    val config = {
      val properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, THROUGHPUT_APP_ID)
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER)
      properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET)
      properties
    }

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

          if (currentMax == -1 || throughput < currentMax) {
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
      }
    ).mapValues(
      new ValueMapper[JsonNode, JsonNode]() {
        def apply(oldVal: JsonNode): JsonNode = {
          val minBytes = oldVal.get("min").asDouble
          val maxBytes = oldVal.get("Max").asDouble
          val throughputpbs = (maxBytes - minBytes) * 8 / 10

          oldVal.asInstanceOf[ObjectNode].without(List("min", "max").asJava).asInstanceOf[ObjectNode].set(
            "throughput_bps",
            JsonNodeFactory.instance.numberNode(throughputpbs)
          )
          return oldVal
        }
      }
    )

    val streamApp : KafkaStreams = new KafkaStreams(builder.build(), config)
    streamApp.start();
  }

}
