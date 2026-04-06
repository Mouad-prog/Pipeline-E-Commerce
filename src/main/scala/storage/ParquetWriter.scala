package storage

import org.apache.spark.sql.{DataFrame, SaveMode}

object ParquetWriter {

  /**
   * Writes a DataFrame to a Parquet file
   *
   * @param df DataFrame to write
   * @param path Target filepath
   * @param mode SaveMode (Overwrite by default)
   */
  def write(df: DataFrame, path: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    df.write
      .mode(mode)
      .parquet(path)
  }
}
