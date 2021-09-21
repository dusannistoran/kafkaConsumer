package kafkaConsumer

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory

class Consumer(topic: String, brokers: String) {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("kafkaConsumer")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val mySchema = StructType(
    StructField("id", IntegerType) ::
      StructField("firstName", StringType) ::
      StructField("lastName", StringType) ::
      StructField("yearOfBirth", IntegerType) ::
      StructField("address", StructType(
        StructField("country", StringType) ::
          StructField("city", StringType) ::
          StructField("street", StringType) :: Nil
      )) ::
      StructField("gender", StringType) :: Nil
  )

  /*
  def streamJsonFromKafkaToConsole(): Unit = {
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    val valueDf = df.selectExpr("CAST(value AS STRING)")

    import org.apache.spark.sql.functions._
    val dfWithColumns = valueDf
      .withColumn("value", from_json(col("value"), mySchema))

    val data = dfWithColumns
      .withColumn("id", dfWithColumns.col("value.id"))
      .withColumn("first_name", dfWithColumns.col("value.firstName"))
      .withColumn("last_name", dfWithColumns.col("value.lastName"))
      .withColumn("year_of_birth", dfWithColumns.col("value.yearOfBirth"))
      .withColumn("address_country", dfWithColumns.col("value.address.country"))
      .withColumn("address_city", dfWithColumns.col("value.address.city"))
      .withColumn("address_street", dfWithColumns.col("value.address.street"))
      .drop("value.address")
      .withColumn("gender", dfWithColumns.col("value.gender"))
      .drop("json")
      .drop("value")

    data.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
  */

  def streamJsonFromKafkaToPostgres(): Unit = {

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()

    val valueDf = df.selectExpr("CAST(value AS STRING)")

    import org.apache.spark.sql.functions._
    val dfWithColumns = valueDf
      .withColumn("value", from_json(col("value"), mySchema))

    val data = dfWithColumns
      .withColumn("id", dfWithColumns.col("value.id"))
      .withColumn("first_name", dfWithColumns.col("value.firstName"))
      .withColumn("last_name", dfWithColumns.col("value.lastName"))
      .withColumn("year_of_birth", dfWithColumns.col("value.yearOfBirth"))
      .withColumn("address_country", dfWithColumns.col("value.address.country"))
      .withColumn("address_city", dfWithColumns.col("value.address.city"))
      .withColumn("address_street", dfWithColumns.col("value.address.street"))
      .drop("value.address")
      .withColumn("gender", dfWithColumns.col("value.gender"))
      .drop("json")
      .drop("value")


    def postgresqlOptions: Map[String, String] = Map(
      "dbtable" -> "public.people",
      "user" -> "postgres",
      "password" -> "pAsSwOrD",
      "driver" -> "org.postgresql.Driver",
      "url" -> "jdbc:postgresql://localhost:5432/people"
    )

    // write to Postgresql
    val mode: SaveMode = SaveMode.Append

    data.writeStream
      .foreachBatch { (batch: DataFrame, _: Long) =>
          batch.write
            .format("jdbc")
            .options(postgresqlOptions)
            .mode(mode)
            .save()
      }
      .start()
      .awaitTermination()
  }

}

object Consumer {

  def main(args: Array[String]): Unit = {

    val consumer = new Consumer("test-topic", "localhost:9092")
    consumer.streamJsonFromKafkaToPostgres()
  }

}
