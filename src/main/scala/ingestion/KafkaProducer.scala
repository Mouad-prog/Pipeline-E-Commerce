package ingestion

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{KafkaProducer => JKafkaProducer, ProducerConfig, ProducerRecord, Callback, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import scala.util.{Try, Using}

/**
 * Produces mock e-commerce data to Kafka topics
 * Generates clients, products, and transactions, then publishes them.
 */
object KafkaProducer extends App with LazyLogging {

  val config = ConfigFactory.load().getConfig("pipeline")
  val bootstrapServers = config.getString("kafka.bootstrap-servers")
  val transactionsTopic = config.getString("kafka.topics.transactions")
  val clientsTopic = config.getString("kafka.topics.clients")
  val productsTopic = config.getString("kafka.topics.products")

  val numTransactions = config.getInt("demo.num-transactions")
  val numClients = config.getInt("demo.num-clients")
  val numProducts = config.getInt("demo.num-products")
  val batchSize = config.getInt("demo.batch-size")
  val delayMs = config.getInt("demo.delay-ms")

  logger.info(s"Starting KafkaProducer → $bootstrapServers")
  logger.info(s"Config: $numClients clients, $numProducts products, $numTransactions transactions")

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.ACKS_CONFIG, "all")
  props.put(ProducerConfig.RETRIES_CONFIG, "3")
  props.put(ProducerConfig.LINGER_MS_CONFIG, "10")
  props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
  props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

  val successCount = new AtomicLong(0)
  val errorCount = new AtomicLong(0)

  val callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception != null) {
        errorCount.incrementAndGet()
        logger.error(s"Failed to send record: ${exception.getMessage}")
      } else {
        successCount.incrementAndGet()
      }
    }
  }

  Using(new JKafkaProducer[String, String](props)) { producer =>
    // 1. Generate and send clients
    logger.info(s"Generating $numClients clients...")
    val clients = MockDataGenerator.generateClients(numClients)
    clients.foreach { client =>
      val json = MockDataGenerator.clientToJson(client)
      val record = new ProducerRecord[String, String](clientsTopic, client.client_id, json)
      producer.send(record, callback)
    }
    producer.flush()
    logger.info(s"✓ Sent ${clients.size} clients to topic '$clientsTopic'")

    // 2. Generate and send products
    logger.info(s"Generating $numProducts products...")
    val products = MockDataGenerator.generateProducts(numProducts)
    products.foreach { product =>
      val json = MockDataGenerator.productToJson(product)
      val record = new ProducerRecord[String, String](productsTopic, product.product_id, json)
      producer.send(record, callback)
    }
    producer.flush()
    logger.info(s"✓ Sent ${products.size} products to topic '$productsTopic'")

    // 3. Generate and send transactions in batches
    logger.info(s"Generating $numTransactions transactions in batches of $batchSize...")
    val clientIds = clients.map(_.client_id)
    val productIds = products.map(_.product_id)

    var sent = 0
    while (sent < numTransactions) {
      val batchEnd = math.min(sent + batchSize, numTransactions)
      val batch = (sent until batchEnd).map { _ =>
        MockDataGenerator.generateTransaction(clientIds, productIds)
      }

      batch.foreach { txn =>
        val json = MockDataGenerator.transactionToJson(txn)
        val record = new ProducerRecord[String, String](transactionsTopic, txn.transaction_id, json)
        producer.send(record, callback)
      }
      producer.flush()

      sent = batchEnd
      logger.info(s"  Sent $sent / $numTransactions transactions")

      if (sent < numTransactions && delayMs > 0) {
        Thread.sleep(delayMs)
      }
    }

    logger.info("─" * 50)
    logger.info(s"✓ KafkaProducer finished")
    logger.info(s"  Clients:      ${clients.size}")
    logger.info(s"  Products:     ${products.size}")
    logger.info(s"  Transactions: $numTransactions")
    logger.info(s"  Successful:   ${successCount.get()}")
    logger.info(s"  Errors:       ${errorCount.get()}")

  } match {
    case scala.util.Success(_) =>
      logger.info("Producer closed successfully.")
    case scala.util.Failure(e) =>
      logger.error(s"Producer failed: ${e.getMessage}", e)
      System.exit(1)
  }
}
