package Cl


import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.TypeOf
import org.apache.spark.sql.functions.{explode, split}
/**
 * Custom Receiver that receives data over a socket. Received bytes are interpreted as
 * text and \n delimited lines are considered as records. They are then counted and printed.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.Cl.CustomReceiver localhost 9999`
 */
object CustomReceiver {
  def main(args: Array[String]): Unit = {
    StreamingExamples.setStreamingLogLevels()

    // Create the context with a 1 second batch size
    val spark: SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("CustomReceiver")
      .getOrCreate()

    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))



//    val my_schema = StructType(Seq(
//      StructField("col1", StringType, nullable = false),
//      StructField("col2", StringType, nullable = false),
//    ))
//
//    val emptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], my_schema)
//    println("emptyDf")
//    emptyDF.show()

//luong data 1
    val linesData1 = ssc.receiverStream(new CustomReceiver("localhost", 11000))
    linesData1.flatMap(_.split(" ").map(_.trim))
    linesData1.foreachRDD { rdd =>
      rdd.foreach{ line => {
        val  arrraLine = line.split(",").toList
        import spark.implicits._
        val testRDD = Seq(arrraLine)
        print(testRDD)  // => ĐÃ add data 1 dòng  vào Seq
        val my_schema = StructType(
          List(
            StructField("cot1", StringType, true),
            StructField("cot2", StringType, true),
            StructField("cot3", StringType, true),
            StructField("cot4", StringType, true),
            StructField("cot5", StringType, true),
            StructField("cot6", StringType, true),
          )
        )
        // tôi đang cần hứng data socket luồng 1 để lưu vào Df nhưng hiện nó chưa lưu đc
        val testDF = Seq(line).toDF("cot1","cot2","cot3","cot4","cot5","cot6")  // -> lỗi ko lưu đc vào DF
      testDF.show(5) // -> lỗi ko lưu đc vào DF

      }
      }
    }

     //luong data 2
//    val linesData2 = ssc.receiverStream(new CustomReceiver("localhost", 11001))
//    val words2 = linesData2.flatMap(_.split(","))
//    val wordCounts2 = words2.map(x => (x, 1)).reduceByKey(_ + _)
//    wordCounts2.print()
//   //hieu chua
//    // 2 luong du lieu la nhu n
    ssc.start()
    ssc.awaitTermination()
  }
}



class CustomReceiver(host: String, port: Int)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  def onStart(): Unit = {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run(): Unit = { receive() }
    }.start()
  }

  def onStop(): Unit = {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive(): Unit = {
    var socket: Socket = null
    var userInput: String = null

    try {
      println(s"Connecting to $host : $port")
      socket = new Socket(host, port)
      println(s"Connected to $host : $port")
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      while(true) {
        userInput = reader.readLine()
        println((userInput))
        store(userInput)
      }
      reader.close()
      socket.close()
      println("Stopped receiving")
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        restart(s"Error connecting to $host : $port", e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }
}


