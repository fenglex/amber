package ink.haifeng.spark

import com.typesafe.config.{Config, ConfigFactory}

object ConfigUtils {
  /**
   * ConfigFactory.load() 默认加载classpath下的application.conf,application.json和application.properties文件。
   *
   */
  lazy val load: Config = ConfigFactory.load()
  val LOCAL_RUN = load.getBoolean("local.run")
  val HIVE_METASTORE_URIS = load.getString("hive.metastore.uris")
  val HIVE_DATABASE = load.getString("hive.database")
  val HDFS_CLIENT_LOG_PATH = load.getString("clientlog.hdfs.path")
  val MYSQL_URL = load.getString("mysql.url")
  val MYSQL_USER = load.getString("mysql.user")
  val MYSQL_PASSWORD = load.getString("mysql.password")
  val TOPIC = load.getString("kafka.userloginInfo.topic")
  val USER_PLAY_SONG_TOPIC = load.getString("kafka.userplaysong.topic")
  val KAFKA_BROKERS = load.getString("kafka.cluster")
  val REDIS_HOST = load.getString("redis.host")
  val REDIS_PORT = load.getInt("redis.port")
  val REDIS_OFFSET_DB = load.getInt("redis.offset.db")
  val REDIS_DB = load.getInt("redis.db")
}
