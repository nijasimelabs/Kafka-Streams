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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}


import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode, ArrayNode, TextNode};


object TrafficStreamProcessing {
  def main(args: Array[String]): Unit = {

    val wanOperationalDbTopic: String = "wan_op_db"
    val trafficTopic: String = "traffic"
    val operationalSCPCTopic: String = "operational_scpc"
    val result_stream_key = "channel_group_profile"
    val result_stream_topic = "channel_group_profile_topic"
    val opscpcKey = "opscpc"
    val wandbKey = "wandb"

    val SPOKE = "spoke"
    val REMOTE= "remotename"
    val CHANNEL_GROUPS= "channel_groups"
    val GROUP_NAME = "groupname"
    val GROUP_MEMBERS = "group_members"
    val ChannelGroupProfile = "changroup_profile"

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

     val props = new Properties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("client.id", "ScalaProducerExample")
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val builder: StreamsBuilder = new StreamsBuilder()
    val aggregate_values: ObjectNode = JsonNodeFactory.instance.objectNode();
    val store: ObjectNode = JsonNodeFactory.instance.objectNode();


    //define stream here
    val trafficStream: KStream[String, JsonNode] = builder.stream(trafficTopic, Consumed.`with`(stringSerde, jsonSerde));
    val operationalStream: KStream[String, JsonNode] = builder.stream(operationalSCPCTopic, Consumed.`with`(stringSerde, jsonSerde))
    val wandbStream: KStream[String, JsonNode] = builder.stream(wanOperationalDbTopic, Consumed.`with`(stringSerde, jsonSerde))



    operationalStream.foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {
          store.set(opscpcKey, value)
          var sum_ip_rate:Double = 0;
          for (i <- 0 until value.size()){
              sum_ip_rate = sum_ip_rate+ value.get(0).get("avaiableIPrate").asDouble()
          }
          aggregate_values.put("Total_available_iprate", sum_ip_rate);
        }
      }
    );


    wandbStream.foreach(
      new ForeachAction[String, JsonNode]() {
        override def apply(key: String, value: JsonNode): Unit = {
          store.set(wandbKey, value)
        }
      }
    );


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
          val rootNode = JsonNodeFactory.instance.objectNode()
          val profiles = JsonNodeFactory.instance.arrayNode()
          val result: ObjectNode = JsonNodeFactory.instance.objectNode()
          // FIXME: check if required
          result.set(SPOKE, value.get(REMOTE))
          val channelGroups: ArrayNode = JsonNodeFactory.instance.arrayNode();

          // iterate over aggregated_values to get the link names
          for(link <- aggregate_values.fields) {
            val channel = JsonNodeFactory.instance.objectNode()
            val groupname: TextNode = JsonNodeFactory.instance.textNode(link.getKey())
            channel.set(GROUP_NAME, groupname)
            val members = JsonNodeFactory.instance.arrayNode()
            channel.set(GROUP_MEMBERS, members)
          }

          result.set(CHANNEL_GROUPS, channelGroups)
          profiles.add(result)
          rootNode.put(ChannelGroupProfile, profiles)
          val data = new ProducerRecord[String, String](result_stream_topic, result_stream_key, rootNode.toString())
          producer.send(data)

        }
      });


    val streamApp : KafkaStreams = new KafkaStreams(builder.build(), config)
    streamApp.start();
  }



  def calculateWeight(channel_cir:Double, aggregate_cir:Double, total_ip_rate:Double, channel_iprate:Double):Double={
   val weight =  (channel_cir/aggregate_cir)*(total_ip_rate/channel_iprate)*100;
   return weight;
  }
}