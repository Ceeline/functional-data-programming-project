import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext


object importDroneCSV extends App{

    
    
    val spark = SparkSession.builder
    .appName("drone-project")
    .master("local[*]")
    .getOrCreate()

    println("dans la bonne fonction")

  val rddCSV = spark.sparkContext.textFile("output.csv")

 // type of val RDD :class org.apache.spark.rdd.MapPartitionsRDD
  val rdd = rddCSV.map(line=>{
    line.split(",")
  })
 


  rdd.foreach(f => {
    println(f.mkString(" "))
  })

  println("the total number of row including headers is " + rdd.count)

  //We can use this RDD to make statistics
}