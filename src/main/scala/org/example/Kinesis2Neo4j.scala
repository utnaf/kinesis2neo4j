package main.scala.org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Kinesis2Neo4j {

  def main(args: Array[String]): Unit = {
    // This is the Kinesis Data Stream name you set when creating the stream
    val streamName = "Kinesis2Neo4j"

    // Kinesis endpoint, can be found here https://docs.aws.amazon.com/general/latest/gr/ak.html
    val kinesisEndpoint = "https://kinesis.eu-central-1.amazonaws.com"

    /**
     * Create the Spark Session with a local instance.
     * We can use the streamName as the Spark app name.
     *
     * https://spark.apache.org/docs/latest/sql-getting-started.html
     */
    val ss = SparkSession
      .builder()
      .master("local[*]")
      .appName(streamName)
      .getOrCreate()

    import ss.implicits._

    /**
     * We leverage the Kinesis Connector for Spark
     * https://github.com/qubole/kinesis-sql
     *
     * You can set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY directly
     * in the option, or you can add them as Environment variables
     *
     * NOTE: startingPosition is the starting position in Kinesis to fetch data from.
     * We want just the new data, reference to the Kinesis Connector docs for
     * other possible values.
     */
    val kinesisStream = ss.readStream
      .format("kinesis")
      .option("streamName", streamName)
      .option("endpointUrl", kinesisEndpoint)
      .option("awsAccessKeyId", sys.env.get("AWS_ACCESS_KEY_ID").orNull)
      .option("awsSecretKey", sys.env.get("AWS_SECRET_ACCESS_KEY").orNull)
      .option("startingPosition", "LATEST")
      .load

    val kinesisStreamDataSchema = StructType(Seq(
      StructField("user_name", DataTypes.StringType, nullable = false),
      StructField("user_checkin_time", DataTypes.TimestampType, nullable = false),
      StructField("event_name", DataTypes.StringType, nullable = false),
    ))

    val kinesisData = kinesisStream
      .selectExpr("CAST(data AS STRING)").as[String]
      .withColumn(
        "jsonData",
        from_json(col("data"), kinesisStreamDataSchema)
      )
      .select("jsonData.*")

    val kinesisQuery = kinesisData
      .writeStream
      // connection options
      .format("org.neo4j.spark.DataSource")
      .option("url", "bolt://localhost:7687")
      .option("authentication.type", "basic")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", "password")
      .option("checkpointLocation", "./kinesis2Neo4jCheckpoint")
      // actual writing setup
      .option("save.mode", "Append")
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

    kinesisQuery.awaitTermination()
  }
}
