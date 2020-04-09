import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{DateType}

case class Ticket(
    plate_id      : String,
    issue_date    : String,
    violation_code: Int,
    violation_time: String
)

object importCSV {
    
  def main(args: Array[String]): Unit = {
      
    val spark = SparkSession.builder
    .appName("drone-project")
    .master("local[*]")
    .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext
    val filename = "/home/celine/Documents/fonctionnal_data_programming/nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2017.csv"
    
    // we get the RDD[String] containing the data of the file
    val (access_tickets) = parseTickets(filename, sc)

    val df_small = access_tickets.toDF("Plate ID", "Issue Date", "Violation Code", "Violation Time")
    
    violationCountDF(df_small).show()
    recidivistCountDF(df_small).show()
    dayCountDF(df_small).show()
    violationsPerDay(df_small, "07/20/2016").show()
    spark.stop()
  }

  import org.apache.spark.rdd.RDD

  def parseTickets(filename: String, sc: SparkContext): RDD[Ticket] = {

    // Read and parse file 
    val access_tickets : RDD[Ticket] = sc
                      .textFile(filename)
                      .map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1))      // split at each comma except commas in quotes
                      .map(x => {   // sorting: put 0 when not well-structured and 1 when valid
                        if (x(1).equals("Plate ID") || x(1).contains(',') || x(4).contains(',') || x(19).contains(','))
                          (x, 0)
                        else
                          (Ticket(x(1), x(4), x(5).toInt, x(19)),1)
                      })
                      .filter(s => s._2 == 1)   // only keep valid lines
                      .map(s => s._1.asInstanceOf[Ticket])      //return RDD[Tickets]

    println(s"Read ${access_tickets.count()} lines")
    (access_tickets)
  }

  def violationCountDF(violationListDF : DataFrame) = {
    violationListDF.groupBy("Violation Code").count
  }

  def recidivistCountDF(violationListDF : DataFrame) = {
    violationListDF.groupBy("Plate ID").count
  }

  def dayCountDF(violationListDF : DataFrame) = {
    violationListDF.groupBy("Issue Date").count
  }

  def violationsPerDay(violationListDF : DataFrame, DateSearched : String) = {
    violationListDF.filter(violationListDF("Issue Date") === DateSearched)
  }
}


// Scala: début de code
/*
// each row is an array of strings (the columns in the csv file)
      // val rows = ArrayBuffer[Array[(String, ju.Date, Int, String)]]()
      val rows = ArrayBuffer[Array[(String, String, Int, String)]]()

      val filepath = "/home/celine/Documents/fonctionnal_data_programming/nyc-parking-tickets/Parking_Violations_Issued_-_Fiscal_Year_2017.csv"
      // val filepath = "/home/celine/Documents/fonctionnal_data_programming/nyc-parking-tickets/test.csv"

*/
// fonctionnel mais donne un iterator et fonction toArray fait planter programme car surcharge
/*
try {
        val bufferedSource = Source.fromFile(filepath)
        
        val cols = bufferedSource.getLines.drop(1).map(_.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)).map(x => (x(1),x(4),x(5).toInt,x(19)))
        bufferedSource.close
        
      } catch {
        case e: FileNotFoundException => println("Couldn't find that file.")
        case e: IOException => println("Got an IOException!")
      }
      */

// Error out of memory
/*
      using(Source.fromFile(filepath)) { source =>
        source.getLines.drop(1).foreach{ line =>
          val cols = line.split(",").map(_.trim)

          try {
            rows += Array[(String, String, Int, String)]((cols(1),cols(4),cols(5).toInt,cols(19)))
          } catch {
            case e: NumberFormatException =>
          }
        }
      }

      def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
        try {
          f(resource)
        } finally {
          resource.close()
        }
*/

// Error out of memory
/*
try {
        val bufferedSource = Source.fromFile(filepath)
        
        bufferedSource.getLines.drop(1).foreach{ line =>
          
          val cols = line.split(",").map(_.trim)
          // println ("cols: " + s"${cols(1)}|${cols(2)}|${cols(3)}|${cols(4)}")  
          // we add the columns to the array
          try {
            rows += Array[(String, String, Int, String)]((cols(1),cols(4),cols(5).toInt,cols(19)))
          } catch {
            case e: NumberFormatException =>
          }
          /*
          val format = new java.text.SimpleDateFormat("dd/MM/yyyy")
          // format.parse(x(4).toString)
          try {
            rows += Array[(String, ju.Date, Int, String)]((cols(1),format.parse(cols(4)),cols(5).toInt,cols(19)))
          } catch {
            case e: ParseException => println("Got a ParseException!")
          }
          */
        }

        bufferedSource.close
        
      } catch {
        case e: FileNotFoundException => println("Couldn't find that file.")
        case e: IOException => println("Got an IOException!")
      }
*/


// fonctionnel: output = FEJ5121|05/18/2017|38|0532P
            /* val cols = line.split(",").map(_.trim)
            println(s"${cols(1)}|${cols(4)}|${cols(5)}|${cols(19)}")*/

//ERROR: out of memory
/*
        using(Source.fromFile(filepath)) { source =>
          for (line <- source.getLines) {
              rows += line.split(",").map(_.trim)
          }
        }

        // (2) print the results
        for (row <- rows) {
            println(s"${row(1)}|${row(4)}|${row(5)}|${row(19)}")
        }
        

        def using[A <: { def close(): Unit }, B](resource: A)(f: A => B): B =
        try {
            f(resource)
        } finally {
            resource.close()
        }
        */

// OUTPUT: the schema and the result of the query
/*
        val spark = SparkSession
        .builder
        .appName("SparkSample")
        .master("local")
        .getOrCreate()

        import spark.implicits._

        spark.sparkContext.setLogLevel("ERROR")
        
        val schema = StructType(
          List(
            StructField("Summons Number", IntegerType, true),
            StructField("Plate ID",  StringType, true),
            StructField("Registration State", StringType, true),
            StructField("Plate Type", StringType, true),
            StructField("Issue Date", TimestampType, true),
            StructField("Violation Code", IntegerType, true),
            StructField("Vehicle Body Type", StringType, true),
            StructField("Vehicle Make", StringType, true),
            StructField("Issuing Agency", StringType, true),
            StructField("Street Code1", IntegerType, true),
            StructField("Street Code2", IntegerType, true),
            StructField("Street Code3", IntegerType, true),
            StructField("Vehicle Expiration Date", IntegerType, true),
            StructField("Violation Location", StringType, true),
            StructField("Violation Precinct", IntegerType, true),
            StructField("Issuer Precinct", IntegerType, true),
            StructField("Issuer Code", IntegerType, true),
            StructField("Issuer Command", StringType, true),
            StructField("Issuer Squad", StringType, true),
            StructField("Violation Time", StringType, true),
            StructField("Time First Observed", StringType, true),
            StructField("Violation County", StringType, true),
            StructField("Violation In Front Of Or Opposite", StringType, true),
            StructField("House Number", StringType, true),
            StructField("Street Name", StringType, true),
            StructField("Intersecting Street", StringType, true),
            StructField("Date First Observed", IntegerType, true),
            StructField("Law Section", IntegerType, true),
            StructField("Sub Division", StringType, true),
            StructField("Violation Legal Code", StringType, true),
            StructField("Days Parking In Effect", StringType, true),
            StructField("From Hours In Effect", StringType, true),
            StructField("Vehicle Color", StringType, true),
            StructField("Unregistered Vehicle?", StringType, true),
            StructField("Vehicle Year", IntegerType, true),
            StructField("Meter Number", StringType, true),
            StructField("Feet From Curb", IntegerType, true),
            StructField("Violation Post Code", StringType, true),
            StructField("Violation Description", StringType, true),
            StructField("No Standing or Stopping Violation", StringType, true),
            StructField("Hydrant Violation", StringType, true),
            StructField("Double Parking Violation", StringType, true)
          )
        )

        // schéma contenant uniquement ce dont on a vraiment besoin comme colonne dans les 4 fichiers
        val schemaSmall = StructType(
          List(
            StructField("Plate ID",  StringType, true),
            StructField("Issue Date", TimestampType, true),     // date de l'infraction : 08/04/2013
            StructField("Violation Code", IntegerType, true),
            StructField("Violation Time", StringType, true),    // heure de l'infraction : 0752A
          )
        )

        //val df_small = spark.createDataFrame(spark.sparkContext.parallelize(part), schemaSmall);
        // print("head: " + df_small.head())
        val ds = spark.
         readStream.
         schema(schema).
         format("csv").
         load("file:/home/celine/Documents/fonctionnal_data_programming/nyc-parking-tickets")

        ds.printSchema()

        val msgs =ds.groupBy( "Violation Code").count

        val msgsStream = msgs.
                        writeStream.
                        format("console").
                        outputMode("complete").
                        queryName("textStream").
                        start().awaitTermination()
        
        ds.show()
*/



// ERROR: Out of memory exception
/*
        // Create DataFrame representing the stream of input lines from nyc-parking-tickets
        val df = spark.readStream
        .schema(schema)
        .csv("file:/home/celine/Documents/fonctionnal_data_programming/nyc-parking-tickets")
        
        df.printSchema()    // affiche la structure du dataframe récupéré
        
        // println ("number of rows in the dataset: " + df.count())
        val df_small = df.select("Plate ID", "Issue Date", "Violation Code", "Violation Time")
        df_small.writeStream.outputMode("append").format("console").start().awaitTermination()
*/



// df.collect.foreach(println)
// df.show
/*// Split the lines into element
        val element = df_small.as[String].flatMap(_.split(" "))

        // Generate running element count
        val elementCounts = element.groupBy("Violation Code").count()
        // Start running the query that prints the running counts to the console
        val query = elementCounts.writeStream
        .outputMode("complete")
        .format("console")
        .start()

        query.awaitTermination()*/


/*
        // Create DataFrame representing the stream of input lines from connection to localhost:9999
        /*val lines = spark.readStream
        .csv("src/main/resources/Parking_Violations_Issued_-_Fiscal_Year_2017.csv")
        // file:/home/celine/Documents/fonctionnal_data_programming/projet/project/
        // Split the lines into words
        val words = lines.as[String].flatMap(_.split(" "))

        // Generate running word count
        val wordCounts = words.groupBy("value").count()

        println(wordCounts)
        */

        // schéma contenant uniquement ce dont on a vraiment besoin comme colonne dans les 4 fichiers
        val schemaSmall = StructType(
          List(
            StructField("Plate ID",  StringType, true),
            StructField("Issue Date", DateType, true),     // date de l'infraction : 08/04/2013
            StructField("Violation Code", IntegerType, true),
            StructField("Violation Time", StringType, true),    // heure de l'infraction : 0752A
          )
        )

        
        val df = spark.readStream
        .schema(schema)
        .csv("src/main/resources/Parking_Violations_Issued_-_Fiscal_Year_2017.csv")

        df.printSchema()    // affiche la structure du dataframe récupéré

        println ("\n\n\nHolla\n\n\n")

        df.registerTempTable("tasks")
        val results = spark.sql("select 'Plate ID' from tasks");
        results.show()
        /*
        val groupDF = df.select("Issue Date")
            .groupBy("Issue Date").count()
            .writeStream.outputMode("complete")
            .format("console").start().awaitTermination()
        */
*/

/*    def main(args:Array[String]):Unit = {
        // val file: BufferedSource = Source.fromResource("Parking_Violations_Issued_-_Fiscal_Year_2017.csv")
        val data = Source.fromFile("Parking_Violations_Issued_-_Fiscal_Year_2017.csv").getLines()
        
        
        data.map { line =>
            println(line)
        }
        
    }
    */