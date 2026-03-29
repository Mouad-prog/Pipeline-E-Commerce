package processing

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import java.io.File

/**
 * Spark Structured Streaming consumer that reads from Kafka,
 * cleans data, computes KPIs, and writes Parquet output.
 *
 * For Block 3, this performs a batch read of existing Kafka data.
 * Block 4 adds PostgreSQL writing. Block 5 adds full streaming.
 */
object SparkStreamingConsumer extends App with LazyLogging {

  // Set Hadoop home for Windows compatibility
  System.setProperty("hadoop.home.dir", "C:\\hadoop")

  val config = ConfigFactory.load().getConfig("pipeline")
  val bootstrapServers = config.getString("kafka.bootstrap-servers")
  val transactionsTopic = config.getString("kafka.topics.transactions")
  val clientsTopic = config.getString("kafka.topics.clients")
  val productsTopic = config.getString("kafka.topics.products")
  val parquetDir = config.getString("output.parquet-dir")
  val checkpointDir = config.getString("spark.checkpoint-dir")

  logger.info("Starting SparkStreamingConsumer...")

  val spark = SparkSession.builder()
    .appName(config.getString("spark.app-name"))
    .master(config.getString("spark.master"))
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  try {
    // Read all existing data from Kafka (batch mode for initial load)
    logger.info(s"Reading from Kafka at $bootstrapServers...")

    val transactionsRaw = readKafkaBatch(spark, bootstrapServers, transactionsTopic)
    val clientsRaw = readKafkaBatch(spark, bootstrapServers, clientsTopic)
    val productsRaw = readKafkaBatch(spark, bootstrapServers, productsTopic)

    val txnCount = transactionsRaw.count()
    val cliCount = clientsRaw.count()
    val prdCount = productsRaw.count()
    logger.info(s"Read from Kafka: $txnCount transactions, $cliCount clients, $prdCount products")

    val postgresConfig = config.getConfig("postgres")

    // Clean data
    logger.info("Cleaning data...")
    val cleanedTransactions = DataCleaner.cleanTransactions(transactionsRaw)
    val cleanedClients = DataCleaner.cleanClients(clientsRaw)
    val cleanedProducts = DataCleaner.cleanProducts(productsRaw)

    val cleanTxn = cleanedTransactions.cache()
    val cleanCli = cleanedClients.cache()
    val cleanPrd = cleanedProducts.cache()

    logger.info(s"After cleaning: ${cleanTxn.count()} transactions, ${cleanCli.count()} clients, ${cleanPrd.count()} products")

    // Write cleaned data to Parquet and Postgres
    logger.info("Writing cleaned data to Parquet and Postgres...")
    
    import storage.{ParquetWriter, PostgresWriter}
    import org.apache.spark.sql.SaveMode
    
    // Transactions
    ParquetWriter.write(cleanTxn, s"$parquetDir/transactions")
    PostgresWriter.write(cleanTxn, "transactions", postgresConfig, SaveMode.Overwrite)
    
    // Clients
    ParquetWriter.write(cleanCli, s"$parquetDir/clients")
    PostgresWriter.write(cleanCli, "clients", postgresConfig, SaveMode.Overwrite)
    
    // Products
    ParquetWriter.write(cleanPrd, s"$parquetDir/products")
    PostgresWriter.write(cleanPrd, "products", postgresConfig, SaveMode.Overwrite)

    // Compute KPIs
    val kpis = DataTransformer.computeAll(cleanTxn, cleanCli, cleanPrd)

    // Write KPIs to Parquet and Postgres
    kpis.foreach { case (name, df) =>
      ParquetWriter.write(df, s"$parquetDir/kpis/$name")
      // Update KPI tables, overwriting existing data for the new batch
      PostgresWriter.write(df, name, postgresConfig, SaveMode.Overwrite)
    }

    // Show sample KPIs
    logger.info("─" * 50)
    logger.info("Revenue by Category:")
    kpis("kpi_revenue_by_category")
      .select("category", "total_revenue", "order_count", "avg_order_value")
      .show(10, truncate = false)

    logger.info("Top Products:")
    kpis("kpi_top_products")
      .select("product_name", "units_sold", "total_revenue")
      .show(10, truncate = false)

    logger.info("Customer Segments:")
    kpis("kpi_customer_segments")
      .select("segment", "total_revenue", "order_count", "unique_clients", "avg_spend")
      .show(10, truncate = false)

    // Count output files
    val parquetFiles = countParquetFiles(parquetDir)
    logger.info(s"✓ SparkStreamingConsumer finished. Total Parquet files: $parquetFiles")

    cleanTxn.unpersist()
    cleanCli.unpersist()
    cleanPrd.unpersist()

  } finally {
    spark.stop()
    logger.info("Spark session stopped.")
  }

  /** Read all messages from a Kafka topic in batch mode. */
  private def readKafkaBatch(spark: SparkSession, servers: String, topic: String): DataFrame = {
    spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", servers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load()
      .select(col("value").cast("string").as("value"))
  }

  /** Count .parquet files recursively in a directory. */
  private def countParquetFiles(dir: String): Int = {
    val root = new File(dir)
    if (!root.exists()) return 0
    root.listFiles().toSeq.flatMap { f =>
      if (f.isDirectory) Seq(countParquetFiles(f.getAbsolutePath))
      else if (f.getName.endsWith(".parquet")) Seq(1)
      else Seq(0)
    }.sum
  }
}
