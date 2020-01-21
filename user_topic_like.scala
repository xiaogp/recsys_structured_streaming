package com.mycom.recsys

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.collection.mutable
import org.apache.spark.sql.streaming.ProcessingTime


object user_topic_profile {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("user_topic_profile")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.default.parallelism", "2")
      .getOrCreate()

    val schema = new StructType()
      .add("user", StringType)
      .add("behavior", StringType)
      .add("item", StringType)
      .add("pty3", StringType)
      .add("time", LongType)

    val nestTimestampFormat = "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"
    val jsonOptions = Map("timestampFormat" -> nestTimestampFormat)

    val df = spark.readStream.format("kafka")
      .option("subscribe", "shoppingmall_log")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "latest").load().select(from_json(col("value").cast("string"), schema, jsonOptions).alias("tmp_table"))

    import spark.implicits._

    val toString1 = udf { (window: GenericRowWithSchema) => window.mkString("-") }

    val df2 = df.select(
      $"tmp_table.user".alias("user"),
      $"tmp_table.behavior".alias("behavior"),
      $"tmp_table.item".alias("item"),
      $"tmp_table.pty3".alias("pty3"),
      $"tmp_table.time".alias("time"))
      .filter(!$"user".isin("unk1", "unk2", "unk3", "unk4", "unk5", "unk6", "unk7", "unk8", "unk9"))
      .withColumn("pref", lit(1) / ((unix_timestamp(current_timestamp()) - $"time" / 1000) * 0.001 + 1))
      .map { row =>
        val user = row.getString(0)
        val pty = row.getString(3)
        val pref = row.getDouble(5)
        val datetime = row.getLong(4)
        (user, (pty, pref), datetime)
      }.toDF("user", "tmp", "datetime")
      .withColumn("datetime", to_timestamp($"datetime" / 1000))
      .withWatermark("datetime", "1440 minutes")
      .groupBy($"user", window($"datetime", "30 minutes", "30 minutes") as "window")
      .agg(collect_list("tmp") as "tmp")
      .withColumn("window", toString1($"window"))
      .map { row =>
        val user = row.getString(0).reverse
        val tmp_list = row.getAs[mutable.WrappedArray[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema]](2)
        val tmp_dict = new scala.collection.mutable.HashMap[String, Double]()
        val datetime = row.getString(1)
        for (x <- tmp_list) {
          val key = x(0).toString
          val score = x(1).toString.toDouble
          if (tmp_dict.contains(key: String)) {
            tmp_dict(key: String) += score
          } else {
            tmp_dict(key) = score
          }
        }
        val res = tmp_dict.toSeq.sortWith(_._2 > _._2).slice(0, 3).map(_._1).toBuffer
        // 如果特征不足
        val diff = 3 - res.length
        for (i <- 1 to diff) {
          res ++= Array("null")
        }
        val top1 = res(0)
        val top2 = res(1)
        val top3 = res(2)
        (user, datetime, top1, top2, top3)
      }.toDF("user", "window", "pty1", "pty2", "pty3")

    /**
     * 写入redis
     */
    //    df2.select($"user", $"pty1", $"pty2", $"pty3")
    //      .writeStream.outputMode("update").foreachBatch { (batchDF: DataFrame, batchId: Long) =>
    //      batchDF
    //        .write
    //        .format("org.apache.spark.sql.redis")
    //        .option("table", "topic_like")
    //        .option("key.column", "user")
    //        .mode("append")
    //        .save()
    //    }.start().awaitTermination()

    /**
     * 写入hbase
     */
    df2.select($"user" as "USER", $"pty1" as "PTY1", $"pty2" as "PTY2", $"pty3" as "PTY3")
      .writeStream.outputMode("update").foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF
        .write
        .format("org.apache.phoenix.spark")
        .option("table", "topic_like")
        .option("zkurl", "localhost:2181")
        .mode("overwrite")
        .save()
    }.option("checkpointLocation", "/var/log/spark/user_topic_profile_ckpt").start().awaitTermination()
  }
}







