name := "drone-project"
version := "1.0"
scalaVersion := "2.12.11"
    
libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.12" % "2.4.5",
                        "org.apache.spark" %% "spark-core" % "2.4.5",
                        "org.apache.kafka" % "kafka_2.12" % "2.4.1")                   