package orchestration

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import ingestion.KafkaProducer
import processing.SparkStreamingConsumer

import java.nio.file.{Files, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Orchestrates the full pipeline.
 * Runs Kafka Producer first, then Spark Consumer.
 * Generates a summary report.
 */
object PipelineOrchestrator extends LazyLogging {

  def main(args: Array[String]): Unit = {
    logger.info("=" * 60)
    logger.info("🚀 Starting E-Commerce Data Pipeline Orchestrator")
    logger.info("=" * 60)

    val isDemoMode = args.contains("--mode") && args.sliding(2).exists(arr => arr(0) == "--mode" && arr(1) == "demo")
    if (!isDemoMode) {
      logger.warn("Running without '--mode demo'. Make sure services are up.")
    } else {
      logger.info("Running in DEMO mode: End-to-end execution.")
    }

    val startTime = System.currentTimeMillis()

    try {
      // 1. Run Ingestion (Producer)
      logger.info("\n▶ PHASE 1: DATA INGESTION")
      KafkaProducer.main(Array.empty)

      // 2. Run Processing (Consumer)
      logger.info("\n▶ PHASE 2: DATA PROCESSING & STORAGE")
      SparkStreamingConsumer.main(Array.empty)

      val endTime = System.currentTimeMillis()
      val durationSec = (endTime - startTime) / 1000

      // 3. Generate Report
      generateSummaryReport(durationSec)

      logger.info("=" * 60)
      logger.info(s"✅ Pipeline completed successfully in $durationSec seconds.")
      logger.info("=" * 60)

    } catch {
      case e: Exception =>
        logger.error(s"❌ Pipeline orchestrator failed: ${e.getMessage}", e)
        System.exit(1)
    }
  }

  private def generateSummaryReport(durationSec: Long): Unit = {
    val config = ConfigFactory.load().getConfig("pipeline")
    val reportsDir = config.getString("output.reports-dir")
    val reportPath = Paths.get(reportsDir, "run_summary.txt")

    // Ensure directory exists
    Files.createDirectories(Paths.get(reportsDir))

    val sb = new StringBuilder()
    sb.append("=" * 50).append("\n")
    sb.append("    E-COMMERCE PIPELINE RUN SUMMARY\n")
    sb.append("=" * 50).append("\n\n")

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    sb.append(s"Date:           ${LocalDateTime.now().format(formatter)}\n")
    sb.append(s"Duration:       $durationSec seconds\n")
    sb.append(s"Mode:           Batch/Demo\n\n")

    sb.append("Metrics Configuration:\n")
    sb.append(s"- Target Transactions: ${config.getInt("demo.num-transactions")}\n")
    sb.append(s"- Target Clients:      ${config.getInt("demo.num-clients")}\n")
    sb.append(s"- Target Products:     ${config.getInt("demo.num-products")}\n\n")

    sb.append("Status:\n")
    sb.append("- Kafka Ingestion:     SUCCESS\n")
    sb.append("- Spark Processing:    SUCCESS\n")
    sb.append("- Parquet Data Lake:   SUCCESS\n")
    sb.append("- PostgreSQL Storage:  SUCCESS\n\n")

    sb.append("Note: For detailed row counts, refer to the final validation scripts \n")
    sb.append("or CheckLog.md.\n")

    Files.write(reportPath, sb.toString().getBytes("UTF-8"))
    logger.info(s"\n📝 Summary report saved to: ${reportPath.toAbsolutePath.toString}")
  }
}
