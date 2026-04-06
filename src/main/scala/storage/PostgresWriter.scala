package storage

import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.util.Properties

object PostgresWriter {

  /**
   * Writes a DataFrame to a PostgreSQL table
   * By default uses SaveMode.Append.
   * If SaveMode.Overwrite is used, Spark will drop and recreate the table.
   * For the demo, since we want to reuse the exact schema from init.sql,
   * we will use truncate for Overwrite mode.
   *
   * @param df DataFrame to write
   * @param tableName Name of the target table
   * @param config The application config block containing Postgres settings
   * @param mode SaveMode
   */
  def write(df: DataFrame, tableName: String, config: Config, mode: SaveMode = SaveMode.Append): Unit = {
    val url = config.getString("url")
    
    val props = new Properties()
    props.setProperty("user", config.getString("user"))
    props.setProperty("password", config.getString("password"))
    props.setProperty("driver", config.getString("driver"))
    props.setProperty("stringtype", "unspecified")

    if (mode == SaveMode.Overwrite) {
      // Instead of dropping the table, Spark's JDBC writer can truncate the table natively if specified.
      df.write
        .mode(SaveMode.Overwrite)
        .option("truncate", "true")
        .jdbc(url, tableName, props)
    } else {
      df.write
        .mode(mode)
        .jdbc(url, tableName, props)
    }
  }
}
