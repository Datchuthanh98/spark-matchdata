import akka.event.slf4j.Logger
import akka.remote.transport.AssociationHandle.Unknown
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

object Test {

  def main(args:Array[String]):Unit= {
//     Logger.getLogger("org").setLevel(Level.ERROR)
val spark: SparkSession = SparkSession.builder()
  .master("local[1]")
  .appName("SparkByExamples.com")
  .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000)
    )
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    df.printSchema()
//    df.show()

    val simpleData2 = Seq(("James","Sales","NY",90000,34,10000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
//    val df2 = simpleData2.toDF("employee_name","department","state","salary","age","bonus")
////    df2.show(false)

    val df3 = df.union(simpleData2.toDF())
    df3.show(false)



    val my_schema = StructType(Seq(
      StructField("employee_name", StringType, nullable = false),
      StructField("department", StringType, nullable = false),
      StructField("state", StringType, nullable = false),
      StructField("salary", StringType, nullable = false),
      StructField("age", StringType, nullable = false),
      StructField("bonus", StringType, nullable = false),
    ))

    val emptyDF: DataFrame = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], my_schema)
    println("emptyDf")
    emptyDF.show()

    emptyDF.union(df).show()
  }
}