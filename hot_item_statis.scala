package com.mycom.recsys

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable


object hot_item_statis {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .appName("hot_item_statis")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.default.parallelism", "2")
      .getOrCreate()
    import spark.implicits._

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

    // 热度权重：view 1，addCart 2，buy 3
    val action_score = udf((action: String) => {
      if (action == "view") 1
      else if (action == "addCart") 2
      else if (action == "buy") 3
      else 1
    }: Int)

    val df2 = df.select(
      $"tmp_table.user".alias("user"),
      $"tmp_table.item".alias("item"),
      $"tmp_table.time".alias("time"),
      $"tmp_table.behavior".alias("score"))
      // 增加计数1
      .withColumn("score", action_score($"score"))
      .map { row =>
        val user = row.getString(0)
        val item = row.getString(1)
        val time = row.getLong(2)
        val score = row.getInt(3)
        (user, time, (item, score))
      }.toDF("user", "datetime", "tmp")
      // 增加一个小时时间做groupBy
      .withColumn("datetime", to_timestamp($"datetime" / 1000))
      .withColumn("hour", reverse(date_format($"datetime", "yyyyMMddHH")))
      .withWatermark("datetime", "1440 minutes")
      .groupBy($"hour", window($"datetime", "60 minutes", "60 minutes") as "window")
      .agg(collect_list("tmp") as "tmp")
      .map { row =>
        val hour = row.getString(0)
        val tmp_list = row.getAs[mutable.WrappedArray[org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema]](2)
        val tmp_dict = new scala.collection.mutable.HashMap[String, Long]()
        for (x <- tmp_list) {
          val key = x(0).toString
          val score = x(1).toString.toLong
          if (tmp_dict.contains(key: String)) {
            tmp_dict(key: String) += score
          } else {
            tmp_dict(key) = score
          }
        }
        val res = tmp_dict.toSeq.sortWith(_._2 > _._2).slice(0, 200).map(_._1).mkString("[", ",", "]")
        (hour, res)
      }.toDF("hour", "hot")

    //    df2.writeStream.format("console").outputMode("complete").start().awaitTermination()
    df2.writeStream.outputMode("update").foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      batchDF
        .write
        .format("org.apache.phoenix.spark")
        .option("table", "hot_item_statis")
        .option("zkurl", "localhost:2181")
        .mode("overwrite")
        .save()
    }.option("checkpointLocation", "/var/log/spark/hot_item_statis_ckpt").start()

    spark.streams.awaitAnyTermination()
  }
}

