package com.globaleagle.netbot

object Constants {
  val CONFIG_PATH = "config.path"
  val APP_CONFIG_FILE = "app.properties"
  val PROD_CONFIG_FILE = "producer.properties"
  val CONS_CONFIG_FILE = "consumer.properties"

  val PROFILE_APP_ID = "channel-group-processor"
  val THROUGHPUT_APP_ID = "throughput-processor"

  val BOOTSTRAP_SERVER = "localhost:9092"
  val AUTO_OFFSET = "latest"

  val PROFILE_PRODUCER_ID = "channel-group-producer"
  val THROUGHPUT_PRODUCER_ID = "throughput-producer"

  val TOPIC_WANOPDB = "wan_op_db"
  val TOPIC_TRAFFIC = "traffic"
  val TOPIC_OPSCPC = "operational_scpc"
  val TOPIC_THROUGHPUT = "throughput"

  val PROFILE_RESULT_TOPIC_KEY = "channel_group_profile"
  val PROFILE_RESULT_TOPIC = "channel_group_profile_topic"

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
