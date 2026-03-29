package processing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.typesafe.scalalogging.LazyLogging

/** Computes KPIs from cleaned e-commerce data. */
object DataTransformer extends LazyLogging {

  /**
   * Revenue by product category.
   * Requires: cleaned transactions joined with products.
   */
  def revenueByCategory(transactions: DataFrame, products: DataFrame): DataFrame = {
    transactions
      .join(products, Seq("product_id"), "left")
      .groupBy("category")
      .agg(
        sum("total_amount").as("total_revenue"),
        count("transaction_id").as("order_count"),
        round(avg("total_amount"), 2).as("avg_order_value")
      )
      .withColumn("window_start", current_timestamp())
      .withColumn("window_end", current_timestamp())
      .select("window_start", "window_end", "category", "total_revenue", "order_count", "avg_order_value")
  }

  /**
   * Revenue by client country.
   * Requires: cleaned transactions joined with clients.
   */
  def revenueByCountry(transactions: DataFrame, clients: DataFrame): DataFrame = {
    transactions
      .join(clients, Seq("client_id"), "left")
      .groupBy("country")
      .agg(
        sum("total_amount").as("total_revenue"),
        count("transaction_id").as("order_count"),
        countDistinct("client_id").as("unique_clients")
      )
      .withColumn("window_start", current_timestamp())
      .withColumn("window_end", current_timestamp())
      .select("window_start", "window_end", "country", "total_revenue", "order_count", "unique_clients")
  }

  /**
   * Top products by units sold.
   * Requires: cleaned transactions joined with products.
   */
  def topProducts(transactions: DataFrame, products: DataFrame, limit: Int = 20): DataFrame = {
    transactions
      .join(products, Seq("product_id"), "left")
      .groupBy("product_id", "name")
      .agg(
        sum("quantity").as("units_sold"),
        sum("total_amount").as("total_revenue")
      )
      .orderBy(desc("units_sold"))
      .limit(limit)
      .withColumn("window_start", current_timestamp())
      .withColumn("window_end", current_timestamp())
      .withColumnRenamed("name", "product_name")
      .select("window_start", "window_end", "product_id", "product_name", "units_sold", "total_revenue")
  }

  /**
   * Customer segment analysis.
   * Requires: cleaned transactions joined with clients.
   */
  def customerSegments(transactions: DataFrame, clients: DataFrame): DataFrame = {
    transactions
      .join(clients, Seq("client_id"), "left")
      .groupBy("segment")
      .agg(
        sum("total_amount").as("total_revenue"),
        count("transaction_id").as("order_count"),
        countDistinct("client_id").as("unique_clients"),
        round(avg("total_amount"), 2).as("avg_spend")
      )
      .withColumn("window_start", current_timestamp())
      .withColumn("window_end", current_timestamp())
      .select("window_start", "window_end", "segment", "total_revenue", "order_count", "unique_clients", "avg_spend")
  }

  /** Compute all KPIs and return them as a map. */
  def computeAll(
    transactions: DataFrame,
    clients: DataFrame,
    products: DataFrame
  ): Map[String, DataFrame] = {
    logger.info("Computing KPIs...")

    val revByCat = revenueByCategory(transactions, products)
    val revByCountry = revenueByCountry(transactions, clients)
    val topProds = topProducts(transactions, products)
    val custSegments = customerSegments(transactions, clients)

    logger.info(s"  Revenue by category:  ${revByCat.count()} rows")
    logger.info(s"  Revenue by country:   ${revByCountry.count()} rows")
    logger.info(s"  Top products:         ${topProds.count()} rows")
    logger.info(s"  Customer segments:    ${custSegments.count()} rows")

    Map(
      "kpi_revenue_by_category" -> revByCat,
      "kpi_revenue_by_country"  -> revByCountry,
      "kpi_top_products"        -> topProds,
      "kpi_customer_segments"   -> custSegments
    )
  }
}
