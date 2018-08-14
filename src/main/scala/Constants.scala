object Constants {
  val APP_ID = "stream-application"
  val BOOTSTRAP_SERVER = "localhost:9092"
  val AUTO_OFFSET = "earliest"

  val PRODUCER_ID = "streams-app-producer"

  val TOPIC_WANOPDB = "wan_op_db"
  val TOPIC_TRAFFIC = "traffic"
  val TOPIC_OPSCPC = "operational_scpc"

  val RESULT_TOPIC_KEY = "channel_group_profile"
  val RESULT_TOPIC = "channel_group_profile_topic"

  val KEY_OPSCPC = "opscpc"
  val KEY_WANDB = "wandb"

  val KEY_SPOKE = "spoke"
  val KEY_REMOTE = "remotename"
  val KEY_CHANNEL_GROUPS = "channel_groups"
  val KEY_GROUP_NAME = "groupname"
  val KEY_GROUP_MEMBERS = "group_members"
  val KEY_CHANNEL_GROUP_PROFILE = "changroup_profile"
}
