package main.scala.org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Kinesis2Neo4j {

  def main(args: Array[String]): Unit = {
    val appName = "Kinesis2Neo4j"
    val endpoint = "https://kinesis.eu-central-1.amazonaws.com"

    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName(appName)
      .getOrCreate()

    import ss.implicits._

    val kinesisStream = ss.readStream
      .format("kinesis")
      .option("streamName", appName)
      .option("endpointUrl", endpoint)
      .option("awsAccessKeyId", sys.env.get("AWS_ACCESS_KEY_ID").orNull)
      .option("awsSecretKey", sys.env.get("AWS_SECRET_ACCESS_KEY").orNull)
      .option("startingPosition", "LATEST")
      .load

    val kinesisStreamDataSchema = StructType(Seq(
      StructField("user_name", DataTypes.StringType, nullable = false),
      StructField("user_checkin_time", DataTypes.TimestampType, nullable = false),
      StructField("event_name", DataTypes.StringType, nullable = false),
    ))

    val kinesisQuery = kinesisStream
      .selectExpr("CAST(data AS STRING)").as[String]
      .withColumn("jsonData", from_json(col("data"), kinesisStreamDataSchema))
      .select("jsonData.*")
      .writeStream
      .format("org.neo4j.spark.DataSource")
      .option("url", "bolt://localhost:7687")
      .option("authentication.type", "basic")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "password")
      .option("save.mode", "Append")
      .option("checkpointLocation", "./kinesis2Neo4jCheckpoint")
      .option("relationship", "CHECKED_IN")
      .option("relationship.save.strategy", "keys")
      .option("relationship.properties", "user_checkin_time:at")
      .option("relationship.source.labels", ":Attendee")
      .option("relationship.source.save.mode", "Overwrite")
      .option("relationship.source.node.keys", "user_name:name")
      .option("relationship.target.labels", ":Event")
      .option("relationship.target.save.mode", "Overwrite")
      .option("relationship.target.node.keys", "event_name:name")
      .start()

    Thread.sleep(25 * 1000)

    try {
      kinesisQuery.stop()
    } catch {
      case _: Throwable => println("+++ Successfully aborted");
    }
  }
}
