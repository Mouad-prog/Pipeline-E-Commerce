package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.scalalogging.LazyLogging

/** Validates, deduplicates, and normalizes raw e-commerce data from Kafka. */
object DataCleaner extends LazyLogging {

  // --- Schemas ---

  val transactionSchema: StructType = StructType(Seq(
    StructField("transaction_id", StringType, nullable = false),
    StructField("client_id", StringType, nullable = false),
    StructField("product_id", StringType, nullable = false),
    StructField("quantity", IntegerType, nullable = false),
    StructField("unit_price", DoubleType, nullable = false),
    StructField("total_amount", DoubleType, nullable = false),
    StructField("currency", StringType, nullable = false),
    StructField("payment_method", StringType, nullable = false),
    StructField("status", StringType, nullable = false),
    StructField("transaction_time", StringType, nullable = false)
  ))

  val clientSchema: StructType = StructType(Seq(
    StructField("client_id", StringType, nullable = false),
    StructField("first_name", StringType, nullable = false),
    StructField("last_name", StringType, nullable = false),
    StructField("email", StringType, nullable = false),
    StructField("country", StringType, nullable = false),
    StructField("city", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false),
    StructField("segment", StringType, nullable = false),
    StructField("registered_at", StringType, nullable = false)
  ))

  val productSchema: StructType = StructType(Seq(
    StructField("product_id", StringType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("category", StringType, nullable = false),
    StructField("sub_category", StringType, nullable = true),
    StructField("brand", StringType, nullable = false),
    StructField("price", DoubleType, nullable = false),
    StructField("weight_kg", DoubleType, nullable = true),
    StructField("rating", DoubleType, nullable = true),
    StructField("stock", IntegerType, nullable = false)
  ))

  /** Parse JSON value column and apply schema. */
  def parseJson(df: DataFrame, schema: StructType): DataFrame = {
    df.select(from_json(col("value").cast(StringType), schema).as("data"))
      .select("data.*")
  }

  /** Clean transactions: deduplicate, validate amounts, parse timestamps. */
  def cleanTransactions(df: DataFrame): DataFrame = {
    val parsed = parseJson(df, transactionSchema)

    parsed
      .filter(col("transaction_id").isNotNull && col("transaction_id") =!= "")
      .filter(col("client_id").isNotNull && col("client_id") =!= "")
      .filter(col("product_id").isNotNull && col("product_id") =!= "")
      .filter(col("quantity") > 0)
      .filter(col("unit_price") > 0)
      .filter(col("total_amount") > 0)
      .withColumn("transaction_time", to_timestamp(col("transaction_time")))
      .filter(col("transaction_time").isNotNull)
      .withColumn("currency", upper(trim(col("currency"))))
      .withColumn("payment_method", lower(trim(col("payment_method"))))
      .withColumn("status", lower(trim(col("status"))))
      // Recalculate total for consistency
      .withColumn("total_amount", round(col("quantity") * col("unit_price"), 2))
      .dropDuplicates("transaction_id")
  }

  /** Clean clients: deduplicate, validate ages, normalize strings. */
  def cleanClients(df: DataFrame): DataFrame = {
    val parsed = parseJson(df, clientSchema)

    parsed
      .filter(col("client_id").isNotNull && col("client_id") =!= "")
      .filter(col("email").isNotNull && col("email").contains("@"))
      .filter(col("age") >= 13 && col("age") <= 120)
      .withColumn("first_name", initcap(trim(col("first_name"))))
      .withColumn("last_name", initcap(trim(col("last_name"))))
      .withColumn("email", lower(trim(col("email"))))
      .withColumn("country", trim(col("country")))
      .withColumn("city", trim(col("city")))
      .withColumn("segment", initcap(trim(col("segment"))))
      .withColumn("registered_at", to_timestamp(col("registered_at")))
      .filter(col("registered_at").isNotNull)
      .dropDuplicates("client_id")
  }

  /** Clean products: deduplicate, validate prices, normalize strings. */
  def cleanProducts(df: DataFrame): DataFrame = {
    val parsed = parseJson(df, productSchema)

    parsed
      .filter(col("product_id").isNotNull && col("product_id") =!= "")
      .filter(col("price") > 0)
      .filter(col("stock") >= 0)
      .withColumn("name", trim(col("name")))
      .withColumn("category", initcap(trim(col("category"))))
      .withColumn("sub_category", initcap(trim(col("sub_category"))))
      .withColumn("brand", trim(col("brand")))
      .withColumn("price", round(col("price"), 2))
      .withColumn("weight_kg", round(col("weight_kg"), 3))
      .withColumn("rating",
        when(col("rating") < 1.0, lit(1.0))
          .when(col("rating") > 5.0, lit(5.0))
          .otherwise(round(col("rating"), 2))
      )
      .dropDuplicates("product_id")
  }
}
