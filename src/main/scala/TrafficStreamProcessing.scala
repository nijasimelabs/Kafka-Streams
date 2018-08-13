import java.util.Properties
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{Printed, KStream, KTable, Produced, Serialized, ForeachAction}
import org.apache.kafka.streams.kstream.ValueJoiner
import org.apache.kafka.streams._
import collection.JavaConversions._
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode, ArrayNode, TextNode};


object TrafficStreamProcessing {
  def main(args: Array[String]): Unit = {

    val wanOperationalDbTopic: String = "wan_op_db"
    val trafficTopic: String = "traffic"
    val operationalSCPCTopic: String = "operational_scpc"

    val opscpcKey = "opscpc"
    val wandbKey = "wandb"

    val SPOKE = "spoke"
    val REMOTE= "remotename"
    val CHANNEL_GROUPS= "channel_groups"
    val GROUP_NAME = "groupname"
    val GROUP_MEMBERS = "group_members"

    val stringSerde: Serde[String] = Serdes.String()
    val jsonSerializer: Serializer[JsonNode] = new JsonSerializer()
    val jsonDeserializer: Deserializer[JsonNode] = new JsonDeserializer()
    val jsonSerde: Serde[JsonNode] = Serdes.serdeFrom(jsonSerializer, jsonDeserializer)


    val config = {
      val properties = new Properties()
      properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application")
      properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      properties
    }

    val builder: StreamsBuilder = new StreamsBuilder()
    val aggregate_values: ObjectNode = JsonNodeFactory.instance.objectNode();
    val store: ObjectNode = JsonNodeFactory.instance.objectNode();


    //define stream here
    val trafficStream: KStream[String, JsonNode] = builder.stream(trafficTopic, Consumed.`with`(stringSerde, jsonSerde));
    val operationalStream: KStream[String, JsonNode] = builder.stream(operationalSCPCTopic,
                                                                      Consumed.`with`(stringSerde, jsonSerde))
    val wandbStream: KStream[String, JsonNode] = builder.stream(wanOperationalDbTopic,
                                                                Consumed.`with`(stringSerde, jsonSerde))



    operationalStream.foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {
          store.set(opscpcKey, value)
        }
      }
    )


    wandbStream.foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {
          store.set(wandbKey, value)
        }
      }
    )


    // processing the stream for aggregation
    trafficStream.foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {

          for( x <- 0 until value.size()){

            // calculate agrgegate cir, mir for each link
            // cir should be the sum of cir and max of mir
            var max_mir:Double = 0.0;
            var sum_cir:Double = 0.0;
            var link_name = value.get(x).get("link").asText();
            val link:ObjectNode = JsonNodeFactory.instance.objectNode();

            for ( i <-  0 until value.get(x).get("trafficclass").size()) {

              val cir =  value.get(x).get("trafficclass").get(i).get("cir").asDouble();
              val mir =  value.get(x).get("trafficclass").get(i).get("mir").asDouble()
              sum_cir = sum_cir + cir
              if (max_mir < mir){
                max_mir = mir;
              }
            }

            // json node for cir mir aggregate
            link.put("cir", sum_cir * 1000000)
            link.put("mir", max_mir * 1000000)
            aggregate_values.set(link_name, link)
          }

          // TODO: filter links with dscp values

          val result: ObjectNode = JsonNodeFactory.instance.objectNode()
          // FIXME: check if required
          result.set(SPOKE, value.get(REMOTE))
          val channelGroups: ArrayNode = JsonNodeFactory.instance.arrayNode();

          // iterate over aggregated_values to get the link names
          for(link <- aggregate_values.fields) {
            val profile = JsonNodeFactory.instance.objectNode()
            val groupname: TextNode = JsonNodeFactory.instance.textNode(link.getKey())
            profile.set(GROUP_NAME, groupname)
            val members = JsonNodeFactory.instance.arrayNode()


            profile.set(GROUP_MEMBERS, members)
          }

          result.set(CHANNEL_GROUPS, channelGroups)
        }
      });


    val streamApp : KafkaStreams = new KafkaStreams(builder.build(), config)
    streamApp.start();
  }
}
