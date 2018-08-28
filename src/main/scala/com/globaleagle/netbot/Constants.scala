package com.globaleagle.netbot

object Constants {
  val CONFIG_PATH = "config.path"
  val CONFIG_FILE = "config.properties"
  val APP_CONFIG_FILE = "app.properties"
  val PROD_CONFIG_FILE = "producer.properties"
  val CONS_CONFIG_FILE = "consumer.properties"

  val PROFILE_APP_ID = "channel-group-processor"
  val THROUGHPUT_APP_ID = "throughput-processor"

  val PROFILE_PRODUCER_ID = "channel-group-producer"
  val THROUGHPUT_PRODUCER_ID = "throughput-producer"

  val TOPIC_WANOPDB = "topics.input.wanopdb"
  val TOPIC_TRAFFIC = "topics.input.traffic"
  val TOPIC_OPSCPC = "topics.input.opscpc"
  val TOPIC_THROUGHPUT = "throughput"
  val PROFILE_RESULT_TOPIC = "topic.output.profile"
  val THROUGHPUT_RESULT_TOPIC = "topic.output.throughput"

  val PROFILE_RESULT_TOPIC_KEY = "channel_group_profile"

  val KEY_OPSCPC = "opscpc"
  val KEY_WANDB = "wandb"

  val KEY_SPOKE = "spoke"
  val KEY_REMOTE = "remotename"
  val KEY_CHANNEL_GROUPS = "channel_groups"
  val KEY_GROUP_NAME = "groupname"
  val KEY_GROUP_MEMBERS = "group_members"
  val KEY_CHANNEL_GROUP_PROFILE = "changroup_profile"
}
