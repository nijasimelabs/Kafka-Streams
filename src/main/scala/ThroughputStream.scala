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
import org.apache.kafka.streams.kstream.{KeyValueMapper, KStream, KTable, Produced, Serialized}
import scala.collection.JavaConverters._
import org.apache.kafka.streams.kstream.TimeWindows;

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
    );

    val streamApp : KafkaStreams = new KafkaStreams(builder.build(), config)
    streamApp.start();
  }

}
