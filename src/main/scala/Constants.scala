object Constants {
  val PROFILE_APP_ID = "stream-application-profile"
  val THROUGHPUT_APP_ID = "stream-application-throughput"
  val BOOTSTRAP_SERVER = "localhost:9092"
  val AUTO_OFFSET = "latest"

  val PRODUCER_ID = "streams-app-producer"

  val TOPIC_WANOPDB = "wan_op_db"
  val TOPIC_TRAFFIC = "traffic"
  val TOPIC_OPSCPC = "operational_scpc"
  val TOPIC_THROUGHPUT = "throughput"

  val RESULT_TOPIC_KEY = "channel_group_profile"
  val RESULT_TOPIC = "channel_group_profile_topic"

  val THROUGHPUT_RESULT_TOPIC = "throughput-result"

  val KEY_OPSCPC = "opscpc"
  val KEY_WANDB = "wandb"

  val KEY_SPOKE = "spoke"
  val KEY_REMOTE = "remotename"
  val KEY_CHANNEL_GROUPS = "channel_groups"
  val KEY_GROUP_NAME = "groupname"
  val KEY_GROUP_MEMBERS = "group_members"
  val KEY_CHANNEL_GROUP_PROFILE = "changroup_profile"
}
